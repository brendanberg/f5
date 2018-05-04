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
from collections import Callable, defaultdict
from hashlib import sha1
import base64
import json
# import inspect
from f5.encoding import ModelJSONEncoder
import logging


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

        self.fields = {k: fields.get(k, None) for k in self.columns}
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
        if 'id' in field_dict:
            del field_dict['id']
        sanitized_fields = {k: field_dict[k]
                            for k in field_dict if k in self.columns}
        self.fields.update(sanitized_fields)
        self.dirty = self.dirty.union(sanitized_fields)

    @property
    def is_dirty(self):
        '''
        True if fields have been modified since being marked clean
        '''
        return len(self.dirty) != 0

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
    def hash(self):
        if self.is_dirty:
            raise ValueError(
                'Cannot generate hash on object with unsaved values')

        # TODO: Should we use MessagePack here?
        # Need to make sure MessagePack maintains order of keys...
        str = json.dumps(self.fields, cls=ModelJSONEncoder, sort_keys=True)
        hash = sha1(str.encode('utf-8'))
        return base64.b32encode(hash.digest())

    @property
    def modified_dict(self):
        '''
        Return only fields that have been modified since last update
        '''
        return {k: self.fields[k] for k in self.dirty}
