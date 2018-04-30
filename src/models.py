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

from functools import wraps


def implicitinit(cls):
    '''
    Decorator function to automatically call superclasses' __init__ methods
    prior to calling the class's defined __init__.

    Creates a reference to the class's defined __init__ method and replaces
    that method with a new method that calls __init__ on each of the base
    classes, then calls the original method if it was defined.
    '''
    # TODO: What happens if you wrap a subclass of a wrapped class?
    # Do __init__ methods get called twice?
    classinit = getattr(cls, '__init__', None)

    def __init__(self, *args, **kwargs):
        for base in cls.__bases__:
            init = getattr(base, '__init__', None)
            if init:
                init(self, *args, **kwargs)
        if classinit:
            classinit(self, *args, **kwargs)

    setattr(cls, '__init__', __init__)
    return cls


class Transform(object):

    def __init__(self, xform):
        self.transform = xform

    def __get__(self, obj, type=None):
        return self.transform_dict(obj.fields, self.transform(obj))

    @staticmethod
    def transform_dict(input, transform):
        '''
        Return a tranformed version of the input dictionary.

        Parameters:
                input: a dictionary with keys of type `str`
                transform: a dictionary mapping `str` to `str` or `lambda`

        Returns: a new dictionary with the transform applied.
        '''
        output = {}

        # The `cache_transform` property of the model object either renames
        # a key, deletes it, or transforms the value depending on whether we
        # get a lambda, a string, or None

        for k, v in input.items():
            val_xform = transform.get(k, k)
            if isinstance(val_xform, str):
                output[val_xform] = v
            elif isinstance(val_xform, Callable):
                args = inspect.getargspec(l).args
                if len(args) == 2:
                    key, val = val_xform(k, v)
                    output[key] = val
                elif len(args) == 1:
                    output[k] = val_xform(v)
                elif len(args) == 0:
                    output[k] = val_xform()

        # TODO: This is kind of a hack, but we need a way to add keys
        # specified in the transform dict that aren't named columns

        new_keys = set(transform.keys()) - set(input.keys())

        for k in new_keys:
            val_xform = transform.get(k)

            if isinstance(val_xform, Callable):
                output[k] = val_xform()

        return output


def transform(fn):
    return Transform(fn)


SENTINEL = []


class Model(object):
    '''
    Model instances are either returned populated from service classes or
    are created by the application and then saved by the service. The model
    maintains a set of fields that have been modified so that updating an
    object in the data store only modifies the changed fields.
    '''
    columns = ['id']
    table_name = None
    link_name = None
    service = None
    select_transform = {}

    def __init__(self, fields=None):
        if fields is None:
            fields = {}

        self.fields = {k: None for k in self.columns}

        self.fields.update((k, fields[k]) for k in fields if k in self.columns)
        self.dirty = set()

    def __getitem__(self, key):
        '''
        Retrieve the value for key from fields if key is a valid column name
        '''
        if key not in self.columns:
            raise KeyError("'%s' is not a recognized database column" % key)

        return self.fields[key]

    def __setitem__(self, key, val):
        '''
        Set key equal to val in fields if key is a valid column name
        Marks key as dirty.
        '''
        if key not in self.columns:
            raise KeyError("'%s' is not a recognized database column" % key)
        elif key == 'id':
            raise KeyError("cannot set 'id' manually")

        self.dirty.add(key)
        self.fields[key] = val

    def __delitem__(self, key):
        '''
        Set key to None in fields if key is a valid column name
        Marks key as dirty.
        '''
        if key not in self.columns:
            raise KeyError("'%s' is not a recognized database column" % key)

        self.dirty.add(key)
        self.fields[key] = None

    def __contains__(self, key):
        '''
        Return True if key in fields
        '''
        return key in self.fields

    def __len__(self):
        '''
        Return number of items in fields
        '''
        return len(self.fields)

    def get(self, key, default=SENTINEL):
        if default is SENTINEL:
            return self.fields.get(key)
        else:
            return self.fields.get(key, default)

    def iterkeys(self):
        '''
        Return fields.iterkeys
        '''
        return self.fields.iterkeys()

    def update(self, field_dict):
        '''
        Update values in fields with the supplied dictionary
        Marks all supplied field names as dirty.
        '''
        sanitized_fields = {k: field_dict[k]
                            for k in field_dict if k in self.columns}
        self.fields.update(sanitized_fields)
        self.dirty = self.dirty.union(sanitized_fields)

    @property
    def is_dirty(self):
        '''
        True if fields have been modified since being marked clean
        '''
        return len(self.dirty) == 0

    @property
    def id(self):
        '''
        Convenience property to access the object's id
        '''
        return self.fields['id']

    @id.setter
    def id(self, value):
        '''
        Set the object's id (and don't mark anything dirty)
        '''
        self.fields['id'] = value

    @property
    def modified_dict(self):
        '''
        Return only fields that have been modified since last update
        '''
        return {k: self.fields[k] for k in self.dirty}

    @transform
    def default(self):
        '''
        Return a transformation dictionary specifying how to construct a repre-
        sentation suitible for publicly visible use.
        '''
        # TODO: fix this to work with transform_dict
        return {'id': self.id}

    @transform
    def cache(self):
        return {'id': None}
