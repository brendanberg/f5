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
Datastore Abstraction Classes

Currently just a database context manager. Futurely, will add connection
pools and a Redis context manager.
'''
# pylint: disable=star-args,abstract-class-not-used

from tornado.web import HTTPError
import redis
import pymysql as MySQLdb
from f5.encoding import MessagePackEncoder

# import json
# import logging


# -------------------------------------------------------------------
# Database Wrappers & Somesuch
# -------------------------------------------------------------------

class DatabaseConnectionError(HTTPError):
    '''Tornado error class for database connection errors. There's got to be a
    better way to do this'''
    def __init__(self, log_message=None, *args, **kwargs):
        HTTPError.__init__(self, log_message, *args, **kwargs)

        self.status_code = 500
        self.log_message = log_message
        self.args = args
        self.reason = kwargs.get('reason', None)

        if 'headers' in kwargs:
            self.headers = kwargs['headers']


class Database(object):
    '''Database context manager. Instantiate with database connection
    parameters, including debug mode and read xor write mode flag. Entering the
    context manager opens a connection and returns a tuple containing the
    connection and cursor'''
    # pylint: disable=too-few-public-methods
    def __init__(self, **settings):
        self._debug = bool(settings.pop('debug', False))
        self._mode = settings.pop('mode', 'read')

        config = dict(settings)
        if 'read' in config:
            del config['read']
        if 'write' in config:
            del config['write']

        if self._mode == 'write':
            config.update(settings.get('write', {}))
        else:
            config.update(settings.get('read', {}))

        self._settings = config
        # dict(settings, **settings.get(self._mode, {}))
        self._conn = None
        self._cursor = None

    def __enter__(self):
        "Open a connection and return a tuple containing the connection and cursor"
        self._conn = MySQLdb.connect(**self._settings)

        if self._mode == 'read':
            def not_implemented():
                'lockout function for read connections'
                raise NotImplementedError('Cannot write to a read-only connection')
            self._conn.commit = not_implemented
            self._conn.rollback = not_implemented

        self._cursor = self._conn.cursor(MySQLdb.cursors.DictCursor)
        return (self._conn, self._cursor)

    def __exit__(self, unused_type, unused_value, unused_traceback):
        "Clean up when you're done"
        self._cursor.close()
        self._conn.close()


class Redis(object):
    '''
    Redis context manager. Instantiate with redis server parameters.
    '''
    # pylint: disable=too-few-public-methods
    def __init__(self, **settings):
        self._debug = bool(settings.pop('debug', False))
        self._settings = dict(settings)
        self._conn = None
        self._pool = redis.ConnectionPool(**self._settings)
        self.encoder = MessagePackEncoder()

    def __enter__(self):
        self._conn = redis.StrictRedis(connection_pool=self._pool)
        return self._conn

    def __exit__(self, unused_type, unused_value, unused_traceback):
        # self._conn.close()
        self._conn = None

    def build_key(self, namespace, id=None):
        "Return the redis key for the specified namespace and id"
        if hasattr(namespace, 'table_name') and hasattr(namespace, 'id'):
            return '{0.table_name}:{0.id}'.format(namespace)
        elif id:
            return '{0}:{1}'.format(namespace, id)
        else:
            return namespace

    def set_value(self, key, value):
        '''
        Set a value for a key
        '''
        with self as redis:
            redis.set(key.encode('utf-8'), value.encode('utf-8'))

    def get_value(self, key):
        '''
        Get a value for a key
        '''
        with self as redis:
            value = redis.get(key.encode('utf-8'))

        return str(value, encoding='utf-8')

    def delete_value(self, key):
        '''
        Delete a value for a key
        '''
        with self as redis:
            redis.delete(key.encode('utf-8'))

    def set_hash(self, model, retries=None):
        '''
        Set the hash for an object...
        '''
        obj_key = self.build_key(model)
        hash_key = '{key}:hash'.format(key=obj_key).encode('utf-8')
        model_hash = model.hash

        with self as redis:
            with redis.pipeline() as pipe:
                while 1:
                    try:
                        pipe.watch(hash_key)
                        old_hash = pipe.get(hash_key)

                        pipe.multi()
                        if old_hash:
                            pipe.delete(old_hash)
                        pipe.set(hash_key, model_hash)
                        pipe.set(model_hash, obj_key.encode('utf-8'))
                        pipe.execute()
                        
                        break
                    except WatchError:
                        continue

    def delete_hash(self, model):
        '''
        Delete the hash for an object
        '''
        obj_key = self.build_key(model)
        hash_key = '{key}:hash'.format(key=obj_key).encode('utf-8')

        with self as redis:
            with redis.pipeline() as pipe:
                while 1:
                    try:
                        pipe.watch(hash_key)
                        old_hash = pipe.get(hash_key)

                        pipe.multi()
                        if old_hash:
                            pipe.delete(old_hash)

                        pipe.delete(hash_key)
                        pipe.execute()

                        break
                    except WatchError:
                        continue

    def set_object(self, model):
        '''
        Set values for each of a model's fields in redis.
        '''

        # >>> model.table_name
        # 'person'
        # >>> model.id
        # 42
        # >>> self.transform(model, Storage.cache)
        # {
        #     'name': 'Andy Warhol',
        #     'telephone': '(212) 387-7555',
        #     'location': (40.398, -72.037),
        #     'email': 'andy@warhol.com'
        #     'birthday': datetime.date(1928, 8, 6)
        # }
        # >>> self.construct_kv(self.transform(model, Storage.cache))
        # {
        #     'name': b'\xabAndy Warhol',
        #     'telephone': b'\xae(212) 387-7555',
        #     'location': b'\x92...',
        #     'email': b'\xafandy@warhol.com',
        #     'birthday': b'...'
        # }

        # TODO: Figure out how to encode references in msgpack
        # 'person:42:friends', ['person:12', 'person:92']

        key = self.build_key(model)
        mapitems = dict(model.fields) #.items()
        del mapitems['id']

        mapping = {
            key.encode('utf-8'): self.encoder.encode(val)
            for key, val in mapitems.items()
        }

        with self as redis:
            response = redis.hmset(key, mapping)

        return response

    def get_object(self, model_class, id):
        '''
        Get all field values for a model class specified by id.
        '''

        key = self.build_key(model_class.table_name, id=id)

        with self as redis:
            mapping = redis.hgetall(key)

        if mapping:
            data = {
                str(key, encoding='utf-8'): self.encoder.decode(val)
                for key, val in mapping.items()
            }
            model = model_class(data)
            model.id = int(id)
            return model
        else:
            return None

    def delete_object(self, model):
        '''
        Deletes all values for the specified model in redis.
        '''
        key = self.build_key(model)

        with self as redis:
            response = redis.delete(key)

        return response
