# Written by Brendan Berg
# Copyright (c) 2015 The Electric Eye Company and Brendan Berg
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

'''
Datastore access service base class

Handles database querying, saving, updating, deleting, etc.
'''
# pylint: disable=star-args

# from f5.storage import Database
from f5.models import Model
from f5.dispatch import multimethod
from datetime import datetime
from collections import namedtuple
import re
import logging


# Options = namedtuple('Options', 'present absent')
Bounds = namedtuple('Bounds', ['limit', 'offset'])


def build_select_expression(columns, transform, alias=None):
    '''
    Returns a string of comma-separated MySQL select expressions from a
    list of column names, an optional transform dictionary that maps column
    names to custom select expressions, and an optional alias for the table
    name.
    '''
    if alias is None:
        alias = ''
    else:
        alias = alias + '.'

    def transform_col(column):
        '''
        Return the transformed name for the specified column
        '''
        return transform.get(column, '{{0}}{0}'.format(column))

    return ', '.join(transform_col(col) for col in columns).format(alias)


class ObjectStore(object):
    '''
    An ObjectStore instance maintains a reference to a datastore connection
    pool and provides an interface to query, create, update, and delete models.
    '''
    dispatch = classmethod(multimethod)

    def __init__(self, datastores, mysql_write=None, redis=None):
        if isinstance(datastores, dict):
            self.datastores = datastores
        else:
            self.datastores = {
                'mysql_read': datastores,
                'mysql_write': mysql_write,
                'redis': redis
            }
        self.buffer = []
        self.update_buffer = []
        self._identifier_pattern = None
        self.MAX_BUFFER_SIZE = 1000  # can tweak this constant

    def match_identifier(self, identifier):
        ''' Returns the identifier string if it is a valid MySQL table or
            column name. Use this as a precaution to prevent SQL injection via
            identifier names in queries.

            (This is insanity is necessary because the %s format option in the
            Python MySQL bindings only escapes Python data types being used as
            column values.)'''
        if self._identifier_pattern is None:
            self._identifier_pattern = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

        match = self._identifier_pattern.match(identifier)
        return match and match.group()

    def count(self, result_class):
        '''
        Return the count of all models of the service's type in the data store
        '''
        count_fmt = 'SELECT COUNT(id) AS count FROM `{0}`'

        if 'date_deleted' in result_class.columns:
            count_fmt = count_fmt + ' WHERE date_deleted IS NULL'

        query = count_fmt.format(result_class.table_name)

        with self.datastores['mysql_read'] as (unused_conn, cursor):
            cursor.execute(query)
            result = cursor.fetchone()

        if result:
            return result['count']
        else:
            return None

    def model_with_id(self, result_class, item_id, use_cache=True):
        '''
        Return a model populated by the database object identified by item_id
        '''
        retrieve_fmt = 'SELECT {columns} FROM `{table}` WHERE ID = %s {filter} LIMIT 1'
        filter_clause = ''

        if 'date_deleted' in result_class.columns:
            filter_clause = 'AND date_deleted IS NULL'
        else:
            filter_clause = ''

        columns = result_class.columns
        transform = result_class.select_transform

        query = retrieve_fmt.format(
            columns=build_select_expression(columns, transform),
            table=self.match_identifier(result_class.table_name),
            filter=filter_clause
        )

        if use_cache:
            result = self.datastores['redis'].get_object(result_class, item_id)

            if result:
                # logging.error('Retrieved from cache')
                return result

        with self.datastores['mysql_read'] as (unused_conn, cursor):
            cursor.execute(query, (item_id,))
            # logging.error(cursor.description)
            result = cursor.fetchone()

        if result:
            model = result_class(result)

            if use_cache:
                self.datastores['redis'].set_object(model)

            return model
        else:
            return None

    def model_with_fields(self, result_class, use_cache=True, prevent_deleted=True, **kwargs):
        '''
        Return a model populated by the database object matching the
        intersection of all specified field values
        '''
        retrieve_fmt = 'SELECT {columns} FROM {table} WHERE {whereclause} LIMIT 1'
        expressions = []
        values = []

        for key, val in kwargs.items():
            if val is None:
                expressions.append('{0} IS NULL'.format(key))
            else:
                expressions.append('{0} = %s'.format(key))
                values.append(val)

        if prevent_deleted and 'date_deleted' in result_class.columns:
            expressions.append('date_deleted IS NULL')

        columns = result_class.columns
        transform = result_class.select_transform

        query = retrieve_fmt.format(
            columns=build_select_expression(columns, transform),
            table=self.match_identifier(result_class.table_name),
            whereclause=' AND '.join(expressions)
        )

        # logging.info(query % tuple(values))
        with self.datastores['mysql_read'] as (unused_conn, cursor):
            cursor.execute(query, tuple(values))
            result = cursor.fetchone()

        if result:
            model = result_class(result)

            if use_cache:
                self.datastores['redis'].set_object(model)

            return model
        else:
            return None

    def count_matching_filter(self, result_class, filters, dependencies={}):
        '''
        Return the total filtered item count

        (Convenience wrapper around `retrieve_filtered`)
        '''
        return self.models_matching_filter(result_class, filters, None, dependencies, count_only=True)

    def models_matching_filter(self, result_class, filters, bounds=None,
                               dependencies={}, count_only=False, sort='id', direction='ASC'):
        '''
        Return a list of objects whose attributes match the filter parameters

        The `filters` parameter is a list of tuples in the form:
          (JOIN_TABLE, WHERE_FIELD, OPERATOR, VALUE)
        '''

        def build_join_clause(dependencies, filters):
            '''
            Return a MySQL join clause for filters applied on foreign fields

            The join clause is constructed by iterating over the list of filters
            and looking up any foreign key relationships defined in the
            dependency dictionary.
            '''
            clause_fmt = 'JOIN {to_table} ON {from_table}.{ref} = {to_table}.id'
            pending = [filter[0] for filter in filters]
            completed = set()
            join_clauses = []

            for to_class in pending:
                from_class = dependencies.get(to_class, None)

                if from_class and from_class not in completed:
                    if from_class != result_class:
                        pending.append(from_class)

                    join_clauses.append(clause_fmt.format(
                        to_table=to_class.table_name,
                        from_table=from_class.table_name,
                        ref=to_class.link_name
                    ))

                    completed.add(from_class)

            return ' '.join(reversed(join_clauses))

        def decode_filter_exp(expr):
            '''
            Return a MySQL operator and value for a parsed operator expression
            '''
            def sanitize(v):
                val = v.replace('%', r'\%').replace('_', r'\_')
                return re.sub(r'[ -]', '_', re.sub(r'[!,.\'#]', '', val))

            op, val = expr
            op = op.lower()

            decoded = ({
                # Maps operator expression to MySQL filter clause expression
                ('is', None): ('IS', None),
                ('is not', None): ('IS NOT', None),
                ('is', True): ('!=', 0),
                ('is', False): ('=', 0),
                ('is not', True): ('=', 0),
                ('is not', False): ('!=', 0)
            }).get((op, val))

            if decoded:
                return decoded

            if isinstance(val, str) and op != '!=':
                # Operator string expressions use the regexp ^ and $ characters
                # to indicate start and end positions of the given match string.
                # Here, we convert those values to use the % wildcard used in
                # MySQL `LIKE` expressions.
                sanitized_val = sanitize(val)

                if sanitized_val[0] == '^' and sanitized_val[-1] == '$':
                    return ('LIKE', sanitized_val[1:-1])
                elif sanitized_val[0] == '^':
                    return ('LIKE', '{0}%'.format(sanitized_val[1:]))
                elif sanitized_val[-1] == '$':
                    return ('LIKE', '%{0}'.format(sanitized_val[:-1]))
                else:
                    return ('LIKE', '%{0}%'.format(sanitized_val))
            elif isinstance(val, tuple):
                return (op, [sanitize(v) if isinstance(v, str) else v for v in val])
            else:
                return (op, val)

        columns = result_class.columns
        transform = result_class.select_transform

        retrieve_stmt = '''
            SELECT {columns} FROM `{table_name}` {join_clause}
            WHERE {filter_clause}
        '''

        if 'date_deleted' in result_class.columns:
            retrieve_stmt += 'AND {table_name}.date_deleted IS NULL '

        where_clauses = []
        values = []

        for idx, filter in enumerate(filters):
            cls, field, op, val = filter

            if op == 'and':
                # This is the counterpart to the range hack in `build_op_expr`.
                # We just insert the second half of the comparison into the
                # filter list after the current one and keep going. Yay!
                filters.insert(idx + 1, (cls, field) + val[1])
                op, val = val[0]

            mysql_op, mysql_val = decode_filter_exp((op, val))

            if isinstance(mysql_val, list):
                where_clauses.append('{0}.{1} {2} ({3})'.format(
                    cls.table_name, field, mysql_op, ', '.join(['%s'] * len(mysql_val))
                ))
                values += mysql_val
            else:
                where_clauses.append('{0}.{1} {2} %s'.format(
                    cls.table_name, field, mysql_op))
                values.append(mysql_val)

        if count_only is True:
            cols = 'COUNT(*) AS count'
            vals = tuple(values)
        else:
            cols = build_select_expression(
                columns, transform, alias=result_class.table_name)
            vals = tuple(values)
            retrieve_stmt += 'ORDER BY {table_name}.{sort} {direction}'

            if bounds:
                vals += bounds
                retrieve_stmt += ' LIMIT %s OFFSET %s'

        query = retrieve_stmt.format(
            columns=cols,
            table_name=self.match_identifier(result_class.table_name),
            join_clause=build_join_clause(dependencies, filters),
            filter_clause=' AND '.join(where_clauses),
            sort=sort, direction=direction
        )

        # logging.info(query % vals)

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query, vals)
            if count_only is True:
                results = cursor.fetchone()
            else:
                results = cursor.fetchall()

        if count_only is True:
            return results['count']
        else:
            return [result_class(r) for r in results]

    def models_with_ids(self, result_class, id_list, use_cache=True):
        '''
        Return a list of objects specified by the list of IDs
        '''

        columns = result_class.columns
        transform = result_class.select_transform

        select_statement = '''SELECT {columnset} FROM `{table_name}`
            WHERE id IN ({subquery}) {deleted_clause}
            ORDER BY FIELD(id, {subquery})'''

        if 'date_deleted' in result_class.columns:
            deleted_clause = 'AND date_deleted IS NULL'
        else:
            deleted_clause = ''

        query = select_statement.format(
            columnset=build_select_expression(columns, transform),
            table_name=self.match_identifier(result_class.table_name),
            subquery=', '.join(['%s'] * len(id_list)),
            deleted_clause=deleted_clause
        )

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query, tuple(id_list * 2))
            results = cursor.fetchall()

        models = []

        for r in results:
            model = result_class(r)

            if use_cache:
                self.datastores['redis'].set_object(model)

            models.append(model)

        return models

    def models_in_range(self, result_class, bounds, sort='id', ascending=True, use_cache=True):
        '''
        Return all items from the database, restricted by bounds
        '''
        columns = result_class.columns
        transform = result_class.select_transform

        limits = [bounds.limit, bounds.offset] if bounds else []

        query = '''SELECT {columnset} FROM `{table}` {whereclause}
            ORDER BY {sort} {dir} {limit}'''

        where_clause = 'WHERE date_deleted IS NULL' if 'date_deleted' in result_class.columns else ''

        direction_map = {
            True: 'ASC',
            False: 'DESC'
        }

        parameters = {
            'whereclause': where_clause,
            'columnset': build_select_expression(columns, transform),
            'table': self.match_identifier(result_class.table_name) or '',
            'sort': self.match_identifier(sort) or 'id',
            'dir': direction_map[ascending],
            'limit': 'LIMIT %s OFFSET %s' if bounds else ''
        }

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query.format(**parameters), tuple(limits))
            results = cursor.fetchall()

        models = []

        for r in results:
            model = result_class(r)

            if use_cache:
                self.datastores['redis'].set_object(model)

            models.append(model)

        return models

    def model_referenced_by_model(self, result_class, model, set_attr=None):
        '''
        Return a record of type specified by `result_class` that is referenced
        by the `result_class.link_name` property defined in `model`. If
        `set_attr` is supplied, the resulting list will be assigned to an
        attribute on the model object with the given name.

        Args:
            result_class: the type of object to be returned
            model: the model instance that is the target of the one-to-many
                relationship
            set_attr: (optional) if set, the attribute name to assign the
                results to on the model instance

        Returns:
            the instance referenced by model
        '''
        # The link ID is the `[THIS_TABLE]_id` column in the model
        # So product_service.retrieve_for_model(offering) would create a new
        # product object whose ID is specified as the `product_id` property
        # of the offering object.
        link_id = model.get(result_class.link_name, None)

        if not link_id:
            return None

        columns = result_class.columns
        transform = result_class.select_transform

        query_fmt = '''SELECT {columnset} FROM `{table}` obj WHERE id = %s'''
        query = query_fmt.format(
            columnset=build_select_expression(columns, transform, alias='obj'),
            # ', '.join(transform.get(col, col) for col in columns)
            table=self.match_identifier(result_class.table_name)
        )

        obj = self.datastores['redis'].get_object(result_class, link_id)

        if not obj:
            with self.datastores['mysql_read'] as (_, cursor):
                cursor.execute(query, (link_id,))
                r = cursor.fetchone()
            obj = r and result_class(r)

            if obj:
                self.datastores['redis'].set_object(obj)

        if set_attr is not None:
            setattr(model, set_attr, obj)

        return obj

    def models_referencing_model(self, result_class, model, bounds,
                                 sort='id', ascending=True, set_attr=None):
        '''
        Return a list of all records of the `result_class` type that refer to
        the given model in a one-to-many relationship. If `set_attr` is
        supplied, the resulting list will be assigned to an attribute on the
        model object with the given name.

        Args:
            model: the model instance that is the target of the one-to-many
                relationship
            set_attr: (optional) if set, the attribute name to assign the
                results to on the model instance

        Returns:
            the list of instances that were retreived
        '''
        limits = [bounds.limit, bounds.offset] if bounds else []

        columns = result_class.columns
        transform = result_class.select_transform

        query_fmt = '''SELECT {columnset} FROM `{table}` obj WHERE {link} = %s
            ORDER BY {sort} {dir} {limit}'''

        direction_map = {
            True: 'ASC',
            False: 'DESC'
        }

        parameters = {
            'columnset': build_select_expression(columns, transform, alias='obj'),
            'table': self.match_identifier(result_class.table_name) or '',
            'link': self.match_identifier(model.link_name),
            'sort': self.match_identifier(sort) or 'id',
            'dir': direction_map[ascending],
            'limit': 'LIMIT %s OFFSET %s' if bounds else ''
        }

        query = query_fmt.format(**parameters)

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query, (model.id,) + tuple(limits))
            results = cursor.fetchall()
        objs = [result_class(r) for r in results]

        for obj in objs:
            self.datastores['redis'].set_object(obj)

        if set_attr is not None:
            setattr(model, set_attr, objs)

        return objs

    def models_linked_to_model(self, result_class, model):
        '''Return all entries for the specified model's type
        Note that if the model's table name is not part of a linking table
        the query will fail and you will not go to space today

        Args:
            model: the model instance that is one side of the many-to-many
                relationship
        Returns:
            the list of instances that were retrieved
        '''
        columns = result_class.columns
        transform = result_class.select_transform

        # NOTE: This is
        linking_table_name = "{0}_{1}".format(
            model.table_name, result_class.table_name)

        query_fmt = '''SELECT {columnset} FROM `{table}` tbl_name
            JOIN {link_table} link ON tbl_name.id = link.{self_link_name}
            WHERE link.{other_link_name} = %s'''

        query = query_fmt.format(
            columnset=build_select_expression(
                columns, transform, alias='tbl_name'),
            table=self.match_identifier(result_class.table_name),
            link_table=self.match_identifier(linking_table_name),
            self_link_name=self.match_identifier(result_class.link_name),
            other_link_name=self.match_identifier(model.link_name)
        )

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query, (model.id,))
            results = cursor.fetchall()

        models = []

        for r in results:
            model = result_class(r)
            self.datastores['redis'].set_object(model)
            models.append(model)

        return models

    # TODO: This and model_has_assoc_item need to be renamed.
    def model_assoc_items(self, base_model, items, **extra):
        '''
        Create a record in a linking table for the pair of models
        '''
        if not isinstance(items, list):
            items = [items]

        # N.B: We do this so we can append additional lists that get zipped later
        values = [[base_model.id] * len(items), [item.id for item in items]]

        for val in extra.values():
            if not isinstance(val, list):
                val_list = [val] * len(items)
            else:
                val_list = val

            if len(val_list) != len(items):
                raise ValueError('extra values must be same length as items')

            values.append(val_list)

        first_item = items[0]
        columns = [base_model.link_name, first_item.link_name] + extra.keys()
        value_part = '(' + ', '.join(['%s'] * (2 + len(extra))) + ')'
        values_template = ', '.join([value_part] * len(items))

        statement = 'INSERT INTO {from_name}_{to_name} ({columns}) VALUES {values}'

        query = statement.format(from_name=base_model.table_name,
                                 to_name=first_item.table_name,
                                 columns=', '.join(columns), values=values_template)

        values = list(chain.from_iterable(zip(*values)))

        with self.datastores['mysql_write'] as (conn, cursor):
            cursor.execute(query, tuple(values))
            conn.commit()

    def model_has_assoc_item(self, model, item):
        '''
        Returns true if a record exists in a linking table for the pair of
        records.
        '''
        statement = '''SELECT id FROM `{from_name}_{to_name}`
                WHERE {from_link_name} = %s AND {to_link_name} = %s'''

        query = statemnt.format(
            from_name=model.table_name, to_name=item.table_name,
            from_link_name=model.link_name, to_link_name=item.link_name)

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query, (model.id, item.id))
            result = cursor.fetchone()

        return result is not None

    def model_deassoc_item(self, model, item):
        '''
        Delete a record in a linking table for the pair of models
        '''
        statement = '''DELETE FROM `{from_name}_{to_name}`
                WHERE {from_link_name} = %s AND {to_link_name} = %s'''

        query = statement.format(
            from_name=model.table_name, to_name=item.table_name,
            from_link_name=model.link_name, to_link_name=item.link_name)

        with self.datastores['mysql_write'] as (conn, cursor):
            cursor.execute(query, (model.id, item.id))
            conn.commit()

    def write_custom(self, qry, vals=None):
        # write a custom sql qry to the write database - ugly hack
        with self.datastores['mysql_write'] as (conn, cursor):
            cursor.execute(qry, vals)
            conn.commit()

    def create(self, model):
        '''
        Save a new object by inserting it into the database
        '''
        data = model.fields  # transform('mysql_insert_transform')
        modified = model.modified_dict
        keys = modified.keys()

        query = 'INSERT INTO `{table}` ({key_clause}) VALUES ({value_clause})'
        table_name = self.match_identifier(model.table_name) or ''
        parameters = {
            'table': table_name,
            'key_clause': ', '.join(self.match_identifier(x) for x in keys),
            'value_clause': ', '.join(['%s'] * len(keys))
        }

        select_stmt = '''SELECT * FROM `{table}` WHERE id = %s
            {filter} LIMIT 1'''
        filter_str = 'AND date_deleted IS NULL' if 'date_deleted' in model.columns else ''

        with self.datastores['mysql_write'] as (conn, cursor):
            cursor.execute(query.format(**parameters),
                           tuple(data[k] for k in keys))
            conn.commit()
            model.id = cursor.lastrowid
            cursor.execute(select_stmt.format(
                table=table_name, filter=filter_str), (model.id,))
            result = cursor.fetchone()

        model.update(result)
        model.dirty = set()
        self.datastores['redis'].set_hash(model)
        self.datastores['redis'].set_object(model)

    def update(self, model, set_date_modified=True, refresh=False):
        '''
        Update an existing object in the database
        '''
        if len(model.dirty) == 0:
            return

        if set_date_modified and 'date_modified' in model:
            model['date_modified'] = datetime.now()

        modified = model.modified_dict
        keys = modified.keys()

        atom = '{} = %s'
        set_clause = ', '.join([atom] * len(keys)).format(*keys)
        update_stmt = 'UPDATE `{0}` SET {1} WHERE id = %s'.format(
            self.match_identifier(model.table_name), set_clause)
        vals = list(modified.values()) + [model.id]
        retrieve_stmt = '''SELECT * FROM `{0}` WHERE id = %s
            AND date_deleted IS NULL LIMIT 1'''.format(model.table_name)

        with self.datastores['mysql_write'] as (conn, cursor):
            cursor.execute(update_stmt, tuple(vals))
            conn.commit()

            if refresh is True:
                cursor.execute(retrieve_stmt, (model.id,))
                result = cursor.fetchone()

                if result:
                    model.update(result)
                else:
                    model.id = None

        model.dirty = set()
        self.datastores['redis'].set_hash(model)
        self.datastores['redis'].set_object(model)

    def delete(self, model):
        '''
        Delete an object either by marking it deleted or deleting the row
        '''
        if 'date_deleted' in model:
            model['date_deleted'] = datetime.now()
            self.update(model)
        else:
            delete_stmt = 'DELETE FROM `{0}` WHERE id = %s'.format(
                model.table_name)

            with self.datastores['mysql_write'] as (conn, cursor):
                cursor.execute(delete_stmt, (model.id,))
                conn.commit()
                model.id = None

        self.datastores['redis'].delete_hash(model)
        self.datastores['redis'].delete_object(model)

    def populate(self, model):
        '''
        Abstract method (no-op) to populate the model with additional data
        '''
        # pylint: disable=no-self-use
        return model

    def batch_create(self, model):
        '''
        Fills the buffer with data to be inserted. When the buffer is full,
        it flushes the results
        '''
        data = model.fields
        keys = data.keys()

        self.buffer.append(tuple(data[k] for k in keys))

        if len(self.buffer) == self.MAX_BUFFER_SIZE:
            self.flush('create', model)

    def batch_update(self, model):
        "Updates multiple entries at once"
        data = model.fields
        keys = data.keys()

        self.update_buffer.append(tuple(data[k] for k in keys))

        if len(self.update_buffer) == self.MAX_BUFFER_SIZE:
            self.flush('update', model)

    def flush(self, operation, model):
        "Write everything in the buffer to the database"
        keys = model.fields.keys()

        parameters = {
            'table': self.match_identifier(model.table_name) or '',
            'key_clause': ', '.join(self.match_identifier(x) for x in keys),
            'value_clause': ', '.join(['%s'] * len(keys))
        }

        if operation == 'create':
            query = 'INSERT INTO {table} ({key_clause}) VALUES ({value_clause})'

            if len(self.buffer) > 0:
                with self.datastores['mysql_write'] as (conn, cursor):
                    cursor.executemany(query.format(**parameters), self.buffer)
                    conn.commit()

            self.buffer = []
        elif operation == 'update':
            query = '''INSERT INTO {table} ({key_clause}) VALUES ({value_clause})
            ON DUPLICATE KEY UPDATE {update_clause}'''

            key = self.match_identifier

            parameters['update_clause'] = ', '.join(
                '{col}=VALUES({col})'.format(col=key(x)) for x in keys)

            if len(self.update_buffer) > 0:
                with self.datastores['mysql_write'] as (conn, cursor):
                    cursor.executemany(query.format(
                        **parameters), self.update_buffer)
                    conn.commit()

            self.update_buffer = []
