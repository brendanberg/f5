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
# from collections import namedtuple
import re


# Options = namedtuple('Options', 'present absent')

class Service(object):
	''' Service instances maintain a reference to a datastore connection pool
		and provide an interface to query, create, update, and delete models.'''

	model_class = Model

	def __init__(self, datastores):
		self.datastores = datastores
		self._identifier_pattern = None

	def match_identifier(self, identifier):
		''' Returns the identifier string if it is a valid MySQL table or
			column name. Use this as a precaution to prevent SQL injection via
			identifier names in queries.

			(This is insanity is necessary because the %s format option in the
			Python MySQL bindings only escapes Python data types being used as
			column values.)'''
		if self._identifier_pattern is None:
			self._identifier_pattern = re.compile(r'^[a-zA-Z_]+$')

		match = self._identifier_pattern.match(identifier)
		return match and match.group()

	def count(self):
		"Return the count of all models of the service's type in the data store"
		query = 'SELECT COUNT(id) FROM {0} WHERE date_deleted IS NULL'.format(
			self.model_class.table_name)

		with self.datastores['mysql_read'] as (unused_conn, cursor):
			cursor.execute(query)
			result = cursor.fetchone()

		if result:
			return result['COUNT(id)']
		else:
			return None

	def retrieve_by_id(self, item_id):
		"Return a model populated by the database object identified by item_id"
		retrieve_stmt = 'SELECT * FROM {0} WHERE ID = %s AND date_deleted IS NULL LIMIT 1'

		with self.datastores['mysql_read'] as (unused_conn, cursor):
			cursor.execute(retrieve_stmt.format(self.model_class.table_name), (item_id,))
			result = cursor.fetchone()

		if result:
			return self.model_class(result)
		else:
			return None

	def retrieve_by_id_list(self, id_list):
		"Return a list of objects specified by the list of IDs"
		subquery = ', '.join(['%s'] * len(id_list))
		format_string = '''SELECT * FROM {0} WHERE id IN ({1})
			AND date_deleted IS NULL ORDER BY FIELD(id, {1})'''
		retrieve_stmt = format_string.format(self.model_class.table_name, subquery)

		with self.datastores['mysql_read'] as (_, cursor):
			cursor.execute(retrieve_stmt, tuple(id_list * 2))
			results = cursor.fetchall()

		return [self.model_class(r) for r in results]

	def retrieve_all(self, bounds, sort='id', ascending=True):
		"Return all items from the database, restricted by bounds"
		limits = [bounds.limit, bounds.offset] if bounds else []

		query = '''SELECT * FROM {table} WHERE date_deleted IS NULL
			ORDER BY {sort} {dir} {limit}'''

		direction_map = {
			True: 'ASC',
			False: 'DESC'
		}

		parameters = {
			'table': self.match_identifier(self.model_class.table_name) or '',
			'sort': self.match_identifier(sort) or 'id',
			'dir': direction_map[ascending],
			'limit': 'LIMIT %s OFFSET %s' if bounds else ''
		}

		with self.datastores['mysql_read'] as (_, cursor):
			cursor.execute(query.format(**parameters), tuple(limits))
			results = cursor.fetchall()

		return [self.model_class(r) for r in results]

	def retrieve_list_for_model(self, model):
		'''Return all entries for the specified model's type
		Note that if the model's table name is not part of a linking table
		the query will fail and you will not go to space today'''

		linking_table_name = "{0}_{1}".format(
			model.table_name, self.model_class.table_name)

		query_fmt = '''SELECT tbl_name.* FROM {0} tbl_name
			JOIN {1} link ON tbl_name.id = link.{2}
			WHERE link.{3} = %s'''

		query = query_fmt.format(
			self.match_identifier(self.model_class.table_name),
			self.match_identifier(linking_table_name),
			self.match_identifier(self.model_class.link_name),
			self.match_identifier(model.link_name)
		)

		with self.datastores['mysql_read'] as (_, cursor):
			cursor.execute(query, (model.id,))
			results = cursor.fetchall()

		return [self.model_class(r) for r in results]

	def create(self, model):
		"Save a new object by inserting it into the database"
		data = model.fields
		keys = data.keys()

		query = 'INSERT INTO {table} ({key_clause}) VALUES ({value_clause})'

		parameters = {
			'table': self.match_identifier(model.table_name) or '',
			'key_clause': ', '.join(self.match_identifier(x) for x in keys),
			'value_clause': ', '.join(['%s'] * len(keys))
		}

		with self.datastores['mysql_write'] as (conn, cursor):
			cursor.execute(query.format(**parameters), tuple(data[k] for k in keys))
			conn.commit()
			model.id = cursor.lastrowid

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
		vals = modified.values() + [model.id]
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

	def delete(self, model):
		"Delete an object either by marking it deleted or deleting the row"
		if 'date_deleted' in model:
			model['date_deleted'] = datetime.now()
			self.update(model)
		else:
			delete_stmt = 'DELETE FROM {0} WHERE id = %s'.format(model.table_name)

			with self.datastores['mysql_write'] as (conn, cursor):
				cursor.execute(delete_stmt, (model.id,))
				conn.commit()
				model.id = None

	def populate(self, model):
		"Abstract method (no-op) to populate the model with additional data"
		# pylint: disable=no-self-use
		return model
