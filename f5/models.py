# ==============================================================================
# Web Core
#
# Data model base class
# ------------------------------------------------------------------------------
# Written by Brendan Berg
# Copyright 2015, Brendan Berg

''' WebCore model base class

Data models are Python objects that offer an interface between datastore
implementations and the semantics of working with data in an application.

IT IS IMPORTANT THAT THERE IS NO MAGIC HERE.

When working with MySQL, the model subclass defines a table name and a list of
column names. An instance maintains a dictionary of fields and their values
as well as a set of field names that have been modified by the application.

Models are retrieved from the database, and created, updated, and deleted  by
a service class, which builds queries from the structure defined in the model
class.

Examples:
	- Instantiating a model is done by passing a dictionary to the __init__
		method. This populates the fields dictionary with the contents of the
		supplied dictionary while ignoring field names that are not defined
		database columns.

		```
		result = cursor.fetchone()
		foo = FooModel(result)
		```

	- Update values using square bracket attribute syntax.
		```
		foo['bar_name'] = 'Eastern Bloc'
		foo.dirty
		# -> set(['bar_name'])
		```
'''


class Model(object):
	''' Model instances are either returned populated from service classes or
		are created by the application and then saved by the service. The model
		maintains a set of fields that have been modified so that updating an
		object in the data store only modifies the changed fields.'''

	columns = ['id']
	table_name = None
	link_name = None
	service = None

	def __init__(self, fields=None):
		if fields is None:
			fields = {}

		self.fields = {k: None for k in self.columns}

		self.fields.update((k, fields[k]) for k in fields if k in self.columns)
		self.dirty = set()

	def __getitem__(self, key):
		"Retrieve the value for key from fields if key is a valid column name"
		if key not in self.columns:
			raise KeyError("'%s' is not a recognized database column" % key)

		return self.fields[key]

	def __setitem__(self, key, val):
		''' Set key equal to val in fields if key is a valid column name
			Marks key as dirty.'''
		if key not in self.columns:
			raise KeyError("'%s' is not a recognized database column" % key)
		elif key == 'id':
			raise KeyError("cannot set 'id' manually")

		self.dirty.add(key)
		self.fields[key] = val

	def __delitem__(self, key):
		''' Set key to None in fields if key is a valid column name
			Marks key as dirty.'''
		if key not in self.columns:
			raise KeyError("'%s' is not a recognized database column" % key)

		self.dirty.add(key)
		self.fields[key] = None

	def __contains__(self, key):
		"Return True if key in fields"
		return key in self.fields

	def __len__(self):
		"Return number of items in fields"
		return len(self.fields)

	def iterkeys(self):
		"Return fields.iterkeys"
		return self.fields.iterkeys()

	def update(self, field_dict):
		''' Update values in fields with the supplied dictionary
			Marks all supplied field names as dirty.'''
		sanitized_fields = {k: field_dict[k] for k in field_dict if k in self.columns}
		self.fields.update(sanitized_fields)
		self.dirty = self.dirty.union(sanitized_fields)

	@property
	def is_dirty(self):
		"True if fields have been modified since being marked clean"
		return len(self.dirty) == 0

	@property
	def id(self):
		"Convenience property to access the object's id"
		return self.fields['id']

	@id.setter
	def id(self, value):
		"Set the object's id (and don't mark anything dirty)"
		self.fields['id'] = value

	@property
	def modified_dict(self):
		"Return only fields that have been modified since last update"
		return {k: self.fields[k] for k in self.dirty}

	@property
	def public_dict(self):
		"Return publicly visible fields. Override to expose more."
		return {'id': self['id']}

