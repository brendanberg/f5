# Written by Brendan Berg
# Copyright 2015, The Electric Eye Company

'''
Datastore access service base class

Handles database querying, saving, updating, deleting, etc.
'''
# pylint: disable=star-args

# from core.storage import Database
from core.models import Model
from datetime import datetime
from collections import namedtuple
from itertools import chain
import logging
import re


# Options = namedtuple('Options', ['present', 'absent'])
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


class Service(object):
    '''
    Service instances maintain a reference to a datastore connection pool
    and provide an interface to query, create, update, and delete models.
    '''

    model_class = Model

    def __init__(self, datastores):
        self.datastores = datastores
        self._identifier_pattern = None

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
            self._identifier_pattern = re.compile(r'^[a-zA-Z_]+$')

        match = self._identifier_pattern.match(identifier)
        return match and match.group()

    def count(self):
        '''
        Return the count of all models of the service's type in the data store
        '''
        count_fmt = 'SELECT COUNT(id) AS count FROM {0}'

        if 'date_deleted' in self.model_class.columns:
            count_fmt = count_fmt + ' WHERE date_deleted IS NULL'

        query = count_fmt.format(self.model_class.table_name)

        with self.datastores['postgres_read'] as (conn, _):
            row = conn.execute(query)
            result = row.fetchone()

        if result:
            return result['count']
        else:
            return None

    def retrieve_by_id(self, item_id):
        '''
        Return a model populated by the database object identified by item_id
        '''
        retrieve_stmt = 'SELECT * FROM {table} WHERE ID = %s {filter} LIMIT 1'
        filter_clause = ''

        if 'date_deleted' in self.model_class.columns:
            filter_clause = 'AND date_deleted IS NULL'

        query = retrieve_stmt.format(
            table=self.model_class.table_name,
            filter=filter_clause)

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(query, (item_id,))
            result = rows.fetchone()

        if result:
            return self.model_class(dict(result.items()))
        else:
            return None

    def retrieve_by_id_list(self, id_list):
        '''
        Return a list of objects specified by the list of IDs
        '''
        subquery = ', '.join(['%s'] * len(id_list))
        format_string = '''SELECT * FROM {0} WHERE id IN ({1})
			AND date_deleted IS NULL ORDER BY FIELD(id, {1})'''
        retrieve_stmt = format_string.format(
            self.model_class.table_name, subquery)

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(retrieve_stmt, tuple(id_list * 2))
            results = rows.fetchall()

        return [self.model_class(dict(r.items())) for r in results]

    def retrieve_all(self, bounds, sort='id', ascending=True):
        '''
        Return all items from the database, restricted by bounds
        '''
        limits = [bounds.limit, bounds.offset] if bounds else []

        query = '''SELECT * FROM {table} {where_clause}
			ORDER BY {sort} {dir} {limit}'''

        where_clause = ''
        if 'date_deleted' in self.model_class.columns:
            where_clause = 'WHERE date_deleted IS NULL'

        direction_map = {
            True: 'ASC',
            False: 'DESC'
        }

        parameters = {
            'table': self.match_identifier(self.model_class.table_name) or '',
            'where_clause': where_clause,
            'sort': self.match_identifier(sort) or 'id',
            'dir': direction_map[ascending],
            'limit': 'LIMIT %s OFFSET %s' if bounds else ''
        }

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(query.format(**parameters), tuple(limits))
            results = rows.fetchall()

        return [self.model_class(dict(r.items())) for r in results]

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

        # self.datastores['redis'].get_object(self.model_class, link_id)
        obj = None

        if not obj:
            with self.datastores['postgres_read'] as (conn, transaction):
                rows = conn.execute(query, (link_id,))
                r = rows.fetchone()

            obj = r and self.model_class(dict(r.items()))

            # if obj:
            #	self.datastores['redis'].set_object(obj)

        if set_attr is not None:
            setattr(model, set_attr, obj)

        return obj

    def retrieve_all_for_model(self, model, bounds, sort='id', ascending=True, set_attr=None):
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

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(query, (model.id,) + tuple(limits))
            results = rows.fetchall()
        objs = [self.model_class(dict(r.items())) for r in results]

        for obj in objs:
            self.datastores['redis'].set_object(obj)

        if set_attr is not None:
            setattr(model, set_attr, objs)

        return objs

    def retrieve_list_for_model(self, model):
        '''
        Return all entries for the specified model's type
        Note that if the model's table name is not part of a linking table
        the query will fail and you will not go to space today
        '''

        linking_table_name = "{0}_{1}".format(
            model.table_name, self.model_class.table_name)

        query_fmt = '''SELECT tbl_name.* FROM {0} tbl_name
			JOIN {1} link ON tbl_name.id = link.{2}
			WHERE link.{3} = %s ORDER BY link.id'''

        query = query_fmt.format(
            self.match_identifier(self.model_class.table_name),
            self.match_identifier(linking_table_name),
            self.match_identifier(self.model_class.link_name),
            self.match_identifier(model.link_name)
        )

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(query, (model.id,))
            results = rows.fetchall()

        return [self.model_class(dict(r.items())) for r in results]

    def create(self, model):
        '''
        Save a new object by inserting it into the database
        '''
        data = model.fields
        keys = [k for k in data.keys() if k != 'id' and model[k] is not None]

        query_fmt = '''INSERT INTO {table} ({key_clause})
				VALUES ({value_clause}) RETURNING id AS id'''

        parameters = {
            'table': self.match_identifier(model.table_name) or '',
            'key_clause': ', '.join(self.match_identifier(x) for x in keys),
            'value_clause': ', '.join(['%s'] * len(keys))
        }

        query = query_fmt.format(**parameters)
        with self.datastores['postgres_write'] as (conn, txn):
            result = conn.execute(query, tuple(data[k] for k in keys))
            txn.commit()

        inserted = result.fetchone()
        model.id = inserted['id']

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

        update_stmt = 'UPDATE {0} SET {1} WHERE id = %s'.format(
            self.match_identifier(model.table_name), set_clause)
        vals = modified.values() + [model.id]
        retrieve_stmt = '''SELECT * FROM {0} WHERE id = %s
			AND date_deleted IS NULL LIMIT 1'''.format(model.table_name)

        with self.datastores['postgres_write'] as (cursor, conn):
            cursor.execute(update_stmt, tuple(vals))
            conn.commit()

            if refresh is True:
                row = cursor.execute(retrieve_stmt, (model.id,))
                result = row.fetchone()

                if result:
                    model.update(dict(result.items()))
                else:
                    model.id = None

        model.dirty = set()

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

            with self.datastores['postgres_write'] as (cursor, conn):
                cursor.execute(delete_stmt, (model.id,))
                conn.commit()
                model.id = None

    def populate(self, model):
        '''
        Abstract method (no-op) to populate the model with additional data
        '''
        # pylint: disable=no-self-use
        return model


class DataService(object):
    '''
    Service instances maintain a reference to a datastore connection pool
    and provide an interface to query, create, update, and delete models.
    '''

    def __init__(self, datastores):
        self.datastores = datastores
        self._identifier_pattern = None

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
            self._identifier_pattern = re.compile(r'^[a-zA-Z_]+$')

        match = self._identifier_pattern.match(identifier)
        return match and match.group()

    def count(self, result_class):
        '''
        Return the count of all models of the service's type in the data store
        '''
        count_fmt = 'SELECT COUNT(id) AS count FROM {0}'

        if 'date_deleted' in class_or_model.columns:
            count_fmt = count_fmt + ' WHERE date_deleted IS NULL'

        query = count_fmt.format(result_class.table_name)

        with self.datastores['postgres_read'] as (conn, _):
            row = conn.execute(query)
            result = row.fetchone()

        if result:
            return result['count']
        else:
            return None

    def retrieve_by_id(self, result_class, item_id):
        '''
        Return a model populated by the database object identified by item_id
        '''
        retrieve_stmt = 'SELECT * FROM {table} WHERE ID = %s {filter} LIMIT 1'
        filter_clause = ''

        if 'date_deleted' in result_class.columns:
            filter_clause = 'AND date_deleted IS NULL'

        query = retrieve_stmt.format(
            table=result_class.table_name,
            filter=filter_clause)

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(query, (item_id,))
            result = rows.fetchone()

        if result:
            return result_class(dict(result.items()))
        else:
            return None

    def retrieve_by_id_list(self, result_class, id_list):
        '''
        Return a list of objects specified by the list of IDs
        '''
        subquery = ', '.join(['%s'] * len(id_list))

        def deleted_clause(x): return 'WHERE date_deleted IS NULL' if x else ''
        format_string = '''SELECT tbl.* FROM {table} AS tbl
			JOIN unnest(array[{subq}]) WITH ORDINALITY t(id, ord)
			USING (id) {deleted_clause} ORDER BY t.ord;'''
        retrieve_stmt = format_string.format(
            table=result_class.table_name, subq=subquery,
            deleted_clause=deleted_clause('date_deleted' in result_class.columns))

        # format_string = '''SELECT * FROM {0} WHERE id IN ({1})
        # 	AND date_deleted IS NULL ORDER BY FIELD(id, {1})'''
        # retrieve_stmt = format_string.format(result_class.table_name, subquery)

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(retrieve_stmt, tuple(id_list))
            results = rows.fetchall()

        return [result_class(dict(r.items())) for r in results]

    def retrieve_all(self, result_class, bounds, sort='id', ascending=True):
        '''
        Return all items from the database, restricted by bounds
        '''
        limits = [bounds.limit, bounds.offset] if bounds else []

        query = '''SELECT * FROM {table} {where_clause}
			ORDER BY {sort} {dir} {limit}'''

        where_clause = ''
        if 'date_deleted' in result_class.columns:
            where_clause = 'WHERE date_deleted IS NULL'

        direction_map = {
            True: 'ASC',
            False: 'DESC'
        }

        parameters = {
            'table': self.match_identifier(result_class.table_name) or '',
            'where_clause': where_clause,
            'sort': self.match_identifier(sort) or 'id',
            'dir': direction_map[ascending],
            'limit': 'LIMIT %s OFFSET %s' if bounds else ''
        }

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(query.format(**parameters), tuple(limits))
            results = rows.fetchall()

        return [result_class(dict(r.items())) for r in results]

    def retrieve_for_model(self, result_class, model, set_attr=None):
        # The link ID is the `[THIS_TABLE]_id` column in the model
        # So product_service.retrieve_for_model(offering) would create a new
        # product object whose ID is specified as the `product_id` property
        # of the offering object.
        link_id = model.get(result_class.link_name, None)

        if not link_id:
            return None

        columns = result_class.columns
        transform = result_class.select_transform

        query_fmt = '''SELECT {columnset} FROM {table} obj WHERE id = %s'''
        query = query_fmt.format(
            columnset=build_select_expression(columns, transform, alias='obj'),
            # ', '.join(transform.get(col, col) for col in columns)
            table=self.match_identifier(result_class.table_name)
        )

        # self.datastores['redis'].get_object(result_class, link_id)
        obj = None

        if not obj:
            with self.datastores['postgres_read'] as (conn, transaction):
                rows = conn.execute(query, (link_id,))
                r = rows.fetchone()

            obj = r and result_class(dict(r.items()))

            # if obj:
            #	self.datastores['redis'].set_object(obj)

        if set_attr is not None:
            setattr(model, set_attr, obj)

        return obj

    def retrieve_all_for_model(self, result_class, model, bounds, sort='id', ascending=True, set_attr=None):
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

        columns = result_class.columns
        transform = result_class.select_transform

        query_fmt = '''SELECT {columnset} FROM {table} obj WHERE {link} = %s
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

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(query, (model.id,) + tuple(limits))
            results = rows.fetchall()
        objs = [result_class(dict(r.items())) for r in results]

        for obj in objs:
            self.datastores['redis'].set_object(obj)

        if set_attr is not None:
            setattr(model, set_attr, objs)

        return objs

    def retrieve_list_for_model(self, result_class, model):
        '''
        Return all entries for the specified model's type
        Note that if the model's table name is not part of a linking table
        the query will fail and you will not go to space today
        '''

        linking_table_name = "{0}_{1}".format(
            model.table_name, result_class.table_name)

        query_fmt = '''SELECT tbl_name.* FROM {0} tbl_name
			JOIN {1} link ON tbl_name.id = link.{2}
			WHERE link.{3} = %s ORDER BY link.id'''

        query = query_fmt.format(
            self.match_identifier(result_class.table_name),
            self.match_identifier(linking_table_name),
            self.match_identifier(result_class.link_name),
            self.match_identifier(model.link_name)
        )

        with self.datastores['postgres_read'] as (conn, transaction):
            rows = conn.execute(query, (model.id,))
            results = rows.fetchall()

        return [result_class(dict(r.items())) for r in results]

    def retrieve_filtered_for_action_and_user(self, result_class, action, user,
                                              bounds, sort='id', ascending=True):
        # TODO: The user service should provide this method as a mixin.
        '''
        Return authorizable model objects that allow the specified user to
        perform the stated action
        '''
        # pylint: disable=no-member
        if not (hasattr(result_class, 'is_authorizable') and
                result_class.is_authorizable()):
            raise AttributeError('result_class must be an authorizable model')

        statement = '''SELECT it.* FROM {table} AS it
			WHERE {clauses} ORDER BY {sort} {dir} {limit}'''

        direction_map = {
            True: 'ASC',
            False: 'DESC'
        }
        clauses = []
        parameters = []

        if 'date_deleted' in result_class.columns:
            clauses += ['it.date_deleted IS NULL']

        if user.get('flags', 0) == user.roles['super']:
            pass
        elif user.get('flags', 0) == user.roles['admin']:
            clauses.append('''(it.auth_flags & %s = %s
				OR it.auth_group_id = %s OR it.auth_owner_id = %s)''')
            parameters += [0x4, 0x4, user['auth_group_id'], user.id]
        else:
            clauses.append('''(it.auth_flags & %s = %s
				OR (it.auth_flags & %s = %s AND it.auth_group_id = %s)
				OR it.auth_owner_id = %s)''')
            parameters += [0x4, 0x4, 0x1, 0x1, user['auth_group_id'], user.id]

        query = statement.format(**{
            'table': result_class.table_name,
            'sort': sort or 'id',
            'dir': direction_map[ascending],
            'limit': 'LIMIT %s OFFSET %s' if bounds else '',
            'clauses': ' AND '.join(clauses)
        })

        if bounds:
            parameters += [bounds.limit, bounds.offset]

        with self.datastores['postgres_read'] as (conn, txn):
            rows = conn.execute(query, tuple(parameters))

        return [result_class(dict(r.items())) for r in rows.fetchall()]

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

        with self.datastores['postgres_write'] as (cursor, conn):
            cursor.execute(query, tuple(values))
            conn.commit()

    def create(self, model):
        '''
        Save a new object by inserting it into the database
        '''
        data = model.fields
        keys = [k for k in data.keys() if k != 'id' and model[k] is not None]

        query_fmt = '''INSERT INTO {table} ({key_clause})
				VALUES ({value_clause}) RETURNING id AS id'''

        parameters = {
            'table': self.match_identifier(model.table_name) or '',
            'key_clause': ', '.join(self.match_identifier(x) for x in keys),
            'value_clause': ', '.join(['%s'] * len(keys))
        }

        query = query_fmt.format(**parameters)
        with self.datastores['postgres_write'] as (conn, txn):
            result = conn.execute(query, tuple(data[k] for k in keys))
            txn.commit()

        inserted = result.fetchone()
        model.id = inserted['id']

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

        update_stmt = 'UPDATE {0} SET {1} WHERE id = %s'.format(
            self.match_identifier(model.table_name), set_clause)
        vals = modified.values() + [model.id]
        retrieve_stmt = '''SELECT * FROM {0} WHERE id = %s
			AND date_deleted IS NULL LIMIT 1'''.format(model.table_name)

        with self.datastores['postgres_write'] as (cursor, conn):
            cursor.execute(update_stmt, tuple(vals))
            conn.commit()

            if refresh is True:
                row = cursor.execute(retrieve_stmt, (model.id,))
                result = row.fetchone()

                if result:
                    model.update(dict(result.items()))
                else:
                    model.id = None

        model.dirty = set()

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

            with self.datastores['postgres_write'] as (cursor, conn):
                cursor.execute(delete_stmt, (model.id,))
                conn.commit()
                model.id = None
