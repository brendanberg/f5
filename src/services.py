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

# from core.storage import Database
from core.models import Model
from core.dispatch import multimethod
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
        return transform.get(column, '{{0}}{0}'.format(column))

    return ', '.join(transform_col(col) for col in columns).format(alias)


def decode_filter_exp(expr):
    def sanitize(v):
        val = v.replace('%', r'\%').replace('_', r'\_')
        return re.sub(r'[ -]', '_', re.sub(r'[!,.\'#]', '', val))

    op, val = expr
    op = op.lower()

    decoded = ({
        ('is', None): ('IS', None),
        ('is not', None): ('IS NOT', None),
        ('is', True): ('!=', 0),
        ('is', False): ('=', 0),
        ('is not', True): ('=', 0),
        ('is not', False): ('!=', 0)
    }).get((op, val))

    if decoded:
        return decoded

    if isinstance(val, str):
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


class Service(object):
    '''
    Service instances maintain a reference to a datastore connection pool
    and provide an interface to query, create, update, and delete models.
    '''

    model_class = Model

    dispatch = classmethod(multimethod)

    def __init__(self, datastores):
        self.buffer = {}
        self.datastores = datastores
        self._identifier_pattern = None
        self.MAX_BUFFER_SIZE = 1000  # can tweak this constant

    def match_identifier(self, identifier):
        '''
        Returns the identifier string if it is a valid MySQL table or
        column name. Use this as a precaution to prevent SQL injection via
        identifier names in queries.

        (This is insanity is necessary because the %s format option in the
        Python MySQL bindings only escapes Python data types being used as
        column values.)
        '''
        if self._identifier_pattern is None:
            self._identifier_pattern = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

        match = self._identifier_pattern.match(identifier)
        return match and match.group()

    def count(self):
        '''
        Return the count of all models of the service's type in the data store
        '''
        query = 'SELECT COUNT(id) FROM {0} WHERE {1}'.format(
            self.model_class.table_name,
            'date_deleted IS NULL' if 'date_deleted' in self.model_class.columns else '1')

        with self.datastores['mysql_read'] as (unused_conn, cursor):
            cursor.execute(query)
            result = cursor.fetchone()

        if result:
            return result['COUNT(id)']
        else:
            return None

    def retrieve_by_id(self, item_id, use_cache=True):
        "Return a model populated by the database object identified by item_id"
        return self.retrieve_by_prop(item_id, 'ID', use_cache)

    def retrieve_by_prop(self, prop, prop_name, use_cache=True):
        "Return a model populated by the database object identified by prop_name"
        columns = self.model_class.columns
        transform = self.model_class.select_transform

        retrieve_stmt = '''SELECT {columns} FROM {table_name} WHERE {prop_name} = %s
            {deleted} LIMIT 1'''

        deleted_clause = 'AND date_deleted IS NULL' if 'date_deleted' in self.model_class.columns else ''

        query = retrieve_stmt.format(
            prop_name=prop_name,
            deleted=deleted_clause,
            columns=build_select_expression(columns, transform),
            table_name=self.match_identifier(self.model_class.table_name)
        )

        if use_cache:
            result = self.datastores['redis'].get_object(
                self.model_class, prop)

            if result:
                # logging.error('Retrieved from cache')
                return result

        with self.datastores['mysql_read'] as (unused_conn, cursor):
            cursor.execute(query, (prop,))
            # logging.error(cursor.description)
            result = cursor.fetchone()

        if result:
            model = self.model_class(result)

            if use_cache:
                self.datastores['redis'].set_object(model)

            return model
        else:
            return None

    def count_filtered(self, filters, dependencies={}, join_op='AND'):
        '''
        Return the total filtered item count

        (Convenience wrapper around `retrieve_filtered`)
        '''
        return self.retrieve_filtered(filters, None, dependencies, count_only=True, join_op=join_op)

    def retrieve_filtered(self, filters, bounds, dependencies={}, count_only=False, join_op='AND'):
        '''
        Return a list of objects whose attributes match the filter parameters

        The `filters` parameter is a list of tuples in the form:
          (JOIN_TABLE, WHERE_FIELD, OPERATOR, VALUE)
        '''

        def build_join_clause(dependencies, filters):
            clause_fmt = 'LEFT JOIN {target} ON {dependency}.{ref} = {target}.id'
            pending = [(filter[0], False) for filter in filters]
            completed = set((self.model_class,))
            join_clauses = []

            for target, seen in pending:
                # As we iterate through the list of filters, we want to
                # order the joins from closest to furthest from the origin
                # table. In the case of a table T referencing a dependency
                # D1, which in turn references the primary table P:
                # (T)->(D)->(P), we want to make sure D is joined to P
                # before T is joined to D.

                # We pull a pending filter from the list of filters and
                # look up its dependency. If we've already satisfied the
                # constraint, we skip it.

                dependency = dependencies.get(target, None)
                # With the dependency in hand, we test whether the target has
                # been satisfied

                if dependency and target not in completed:
                    if dependency in completed:
                        # If the dependency is in the set of satisfied targets
                        # (which includes the primary table), it's safe to
                        # append the dependency. We also mark that the target
                        # has been satisfied.
                        join_clauses.append(clause_fmt.format(
                            target=target.table_name,
                            dependency=dependency.table_name,
                            ref=target.link_name
                        ))

                        completed.add(target)
                    elif not seen:
                        # If we haven't seen this target before, we append the
                        # dependency to the pending list (marked 'unseen') and
                        # we append the target (marked 'seen')
                        pending.append((dependency, False))
                        pending.append((target, True))
                    else:
                        # If the target has previously been seen, this is a
                        # dependency that cannot be satisfied. That's an error
                        raise Exception(
                            'unsatisfiable filter dependency: %s -> %s', target, dependency)

            return ' '.join(join_clauses)

        columns = self.model_class.columns
        transform = self.model_class.select_transform

        retrieve_stmt = '''
            SELECT {columns} FROM {table_name} {join_clause}
            WHERE {filter_clause}
        '''

        if 'date_deleted' in self.model_class.columns:
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
                    cls.table_name, field, mysql_op, ', '.join(
                        ['%s'] * len(mysql_val))
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
                columns, transform, alias=self.model_class.table_name)
            vals = tuple(values)
            retrieve_stmt += 'ORDER BY {table_name}.id'

            if bounds:
                vals += bounds
                retrieve_stmt += ' LIMIT %s OFFSET %s'

        query = retrieve_stmt.format(
            columns=cols,
            table_name=self.match_identifier(self.model_class.table_name),
            join_clause=build_join_clause(dependencies, filters),
            filter_clause=' {} '.format(join_op).join(where_clauses)
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
            return [self.model_class(r) for r in results]

    def retrieve_by_prop_list(self, prop_list, prop='id', use_cache=True):
        "Return a list of objects specified by the list of IDs"
        if not prop_list:
            return []

        columns = self.model_class.columns
        transform = self.model_class.select_transform

        select_statement = '''SELECT {columnset} FROM {table_name}
            WHERE {prop} IN ({subquery}) AND date_deleted IS NULL
            ORDER BY FIELD({prop}, {subquery})'''

        query = select_statement.format(
            columnset=build_select_expression(columns, transform),
            table_name=self.match_identifier(self.model_class.table_name),
            subquery=', '.join(['%s'] * len(prop_list)),
            prop=prop
        )

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query, tuple(prop_list * 2))
            results = cursor.fetchall()

        models = []

        for r in results:
            model = self.model_class(r)

            if use_cache:
                self.datastores['redis'].set_object(model)

            models.append(model)

        return models

    def retrieve_by_id_list(self, id_list, use_cache=True):
        "Return a list of objects specified by the list of IDs"
        return self.retrieve_by_prop_list(id_list, prop='id', use_cache=use_cache)

    def retrieve_all(self, bounds, sort='id', ascending=True, use_cache=True):
        "Return all items from the database, restricted by bounds"
        columns = self.model_class.columns
        transform = self.model_class.select_transform

        limits = [bounds.limit, bounds.offset] if bounds else []

        query = 'SELECT {columnset} FROM {table} {whereclause} ORDER BY {sort} {dir} {limit}'

        where_clause = 'WHERE date_deleted IS NULL' if 'date_deleted' in self.model_class.columns else ''

        direction_map = {
            True: 'ASC',
            False: 'DESC'
        }

        parameters = {
            'whereclause': where_clause,
            'columnset': build_select_expression(columns, transform),
            'table': self.match_identifier(self.model_class.table_name) or '',
            'sort': self.match_identifier(sort) or 'id',
            'dir': direction_map[ascending],
            'limit': 'LIMIT %s OFFSET %s' if bounds else ''
        }

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query.format(**parameters), tuple(limits))
            results = cursor.fetchall()

        models = []

        for r in results:
            model = self.model_class(r)

            if use_cache:
                self.datastores['redis'].set_object(model)

            models.append(model)

        return models

    def retrieve_for_model(self, model, set_attr=None):
        # The link ID is the `[THIS_TABLE]_id` column in the model
        # So product_service.retrieve_for_model(offering) would create a new
        # product object whose ID is specified as the `product_id` property
        # of the offering object.
        link_id = model.get(self.model_class.link_name, None)

        if not link_id:
            return None

        columns = self.model_class.columns
        transform = self.model_class.select_transform

        query_fmt = '''SELECT {columnset} FROM {table} obj WHERE id = %s'''
        query = query_fmt.format(
            columnset=build_select_expression(columns, transform, alias='obj'),
            # ', '.join(transform.get(col, col) for col in columns)
            table=self.match_identifier(self.model_class.table_name)
        )

        obj = self.datastores['redis'].get_object(self.model_class, link_id)

        if not obj:
            with self.datastores['mysql_read'] as (_, cursor):
                cursor.execute(query, (link_id,))
                r = cursor.fetchone()
            obj = r and self.model_class(r)

            if obj:
                self.datastores['redis'].set_object(obj)

        if set_attr is not None:
            setattr(model, set_attr, obj)

        return obj

    def retrieve_all_for_model(self, model, bounds=None, sort='id', ascending=True, set_attr=None):
        '''
        Return a list of all records of the service's model type that refer to
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

        columns = self.model_class.columns
        transform = self.model_class.select_transform

        query_fmt = '''SELECT {columnset} FROM {table} obj WHERE {link} = %s
            ORDER BY {sort} {dir} {limit}'''

        direction_map = {
            True: 'ASC',
            False: 'DESC'
        }

        parameters = {
            'columnset': build_select_expression(columns, transform, alias='obj'),
            'table': self.match_identifier(self.model_class.table_name) or '',
            'link': self.match_identifier(model.link_name),
            'sort': self.match_identifier(sort) or 'id',
            'dir': direction_map[ascending],
            'limit': 'LIMIT %s OFFSET %s' if bounds else ''
        }

        query = query_fmt.format(**parameters)

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query, (model.id,) + tuple(limits))
            results = cursor.fetchall()
        objs = [self.model_class(r) for r in results]

        for obj in objs:
            self.datastores['redis'].set_object(obj)

        if set_attr is not None:
            setattr(model, set_attr, objs)

        return objs

    def retrieve_list_for_model(self, model):
        '''Return all entries for the specified model's type
        Note that if the model's table name is not part of a linking table
        the query will fail and you will not go to space today

        Args:
            model: the model instance that is one side of the many-to-many
                relationship
        Returns:
            the list of instances that were retrieved
        '''
        columns = self.model_class.columns
        transform = self.model_class.select_transform

        # NOTE: This is
        linking_table_name = "{0}_{1}".format(
            model.table_name, self.model_class.table_name)

        query_fmt = '''SELECT {columnset} FROM {table} tbl_name
            LEFT JOIN {link_table} link ON tbl_name.id = link.{self_link_name}
            WHERE link.{other_link_name} = %s'''

        query = query_fmt.format(
            columnset=build_select_expression(
                columns, transform, alias='tbl_name'),
            table=self.match_identifier(self.model_class.table_name),
            link_table=self.match_identifier(linking_table_name),
            self_link_name=self.match_identifier(self.model_class.link_name),
            other_link_name=self.match_identifier(model.link_name)
        )

        with self.datastores['mysql_read'] as (_, cursor):
            cursor.execute(query, (model.id,))
            results = cursor.fetchall()

        models = []

        for r in results:
            model = self.model_class(r)
            self.datastores['redis'].set_object(model)
            models.append(model)

        return models

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
        statement = '''SELECT id FROM {from_name}_{to_name}
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
        statement = '''DELETE FROM {from_name}_{to_name}
                WHERE {from_link_name} = %s AND {to_link_name} = %s'''

        query = statement.format(
            from_name=model.table_name, to_name=item.table_name,
            from_link_name=model.link_name, to_link_name=item.link_name)

        with self.datastores['mysql_write'] as (conn, cursor):
            cursor.execute(query, (model.id, item.id))
            conn.commit()

    def create(self, model):
        "Save a new object by inserting it into the database"
        data = model.fields  # transform('mysql_insert_transform')
        keys = data.keys()

        query = 'INSERT INTO {table} ({key_clause}) VALUES ({value_clause})'

        parameters = {
            'table': self.match_identifier(model.table_name) or '',
            'key_clause': ', '.join(self.match_identifier(x) for x in keys),
            'value_clause': ', '.join(['%s'] * len(keys))
        }

        with self.datastores['mysql_write'] as (conn, cursor):
            cursor.execute(query.format(**parameters),
                           tuple(data[k] for k in keys))
            conn.commit()
            model.id = cursor.lastrowid

        model.dirty = set()
        self.datastores['redis'].set_hash(model)
        self.datastores['redis'].set_object(model)

    def update(self, model, set_date_modified=True, refresh=False):
        "Update an existing object in the database"
        if len(model.dirty) == 0:
            return

        if set_date_modified and 'date_modified' in model:
            model['date_modified'] = datetime.now()

        modified = model.modified_dict
        keys = modified.keys()

        atom = '{} = %s'
        set_clause = ', '.join([atom] * len(keys)).format(*keys)

        update_stmt = 'UPDATE {0} SET {1} WHERE id = %s'.format(
            self.match_identifier(model.table_name), set_clause)
        vals = list(modified.values()) + [model.id]
        retrieve_stmt = '''SELECT * FROM {0} WHERE id = %s
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
            delete_stmt = 'DELETE FROM {0} WHERE id = %s'.format(
                model.table_name)

            with self.datastores['mysql_write'] as (conn, cursor):
                cursor.execute(delete_stmt, (model.id,))
                conn.commit()
                model.id = None

        self.datastores['redis'].delete_hash(model)
        self.datastores['redis'].delete_object(model)

    def delete_all(self, models, date_deleted=False):
        '''
        Delete the list of models by marking them deleted or deleting the rows
        '''
        if date_deleted:
            for model in models:
                self.delete(model)
        else:
            delete_stmt = 'DELETE FROM {table_name} WHERE id IN ({subquery})'
            delete_stmt = delete_stmt.format(
                table_name=self.match_identifier(self.model_class.table_name),
                subquery=', '.join(['%s'] * len(models))
            )
            id_list = [model.id for model in models]

            with self.datastores['mysql_write'] as (conn, cursor):
                cursor.execute(delete_stmt, tuple(id_list))
                conn.commit()

            for model in models:
                model.id = None
                self.datastores['redis'].delete_hash(model)
                self.datastores['redis'].delete_object(model)

    def populate(self, model):
        "Abstract method (no-op) to populate the model with additional data"
        # pylint: disable=no-self-use
        return model

    def batch_create(self, model):
        '''
        Fills the buffer with data to be inserted. When the buffer is full,
        it flushes the results
        '''
        data = model.fields
        keys = data.keys()

        query = 'INSERT INTO {table} ({key_clause}) VALUES ({value_clause})'
        parameters = {
            'table': self.match_identifier(model.table_name) or '',
            'key_clause': ', '.join(self.match_identifier(x) for x in keys),
            'value_clause': ', '.join(['%s'] * len(keys))
        }
        query = query.format(**parameters)

        if query in self.buffer:
            self.buffer[query].append(tuple(data[k] for k in keys))
        else:
            self.buffer[query] = [tuple(data[k] for k in keys)]

        if len(self.buffer[query]) == self.MAX_BUFFER_SIZE:
            self.flush()

    def batch_update(self, model):
        "Updates multiple entries at once"
        data = model.fields
        keys = data.keys()

        query = '''
            INSERT INTO {table} ({key_clause}) VALUES ({value_clause})
            ON DUPLICATE KEY UPDATE {update_clause}
        '''
        parameters = {
            'table': self.match_identifier(model.table_name) or '',
            'key_clause': ', '.join(self.match_identifier(x) for x in keys),
            'value_clause': ', '.join(['%s'] * len(keys)),
            'update_clause': ', '.join('{col}=VALUES({col})'.format(col=self.match_identifier(x)) for x in keys)
        }

        query = query.format(**parameters)

        if query in self.buffer:
            self.buffer[query].append(tuple(data[k] for k in keys))
        else:
            self.buffer[query] = [tuple(data[k] for k in keys)]

        if len(self.buffer[query]) == self.MAX_BUFFER_SIZE:
            self.flush()

    def flush(self):
        "Write everything in the buffer to the database"
        with self.datastores['mysql_write'] as (conn, cursor):
            for query in self.buffer:
                try:
                    cursor.executemany(query, self.buffer[query])
                except Exception as e:
                    conn.rollback()
                    self.buffer = {}
                    raise e
                else:
                    conn.commit()

        self.buffer = {}
