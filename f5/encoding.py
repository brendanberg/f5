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
Provides common encodings used in Tornado services.

- JSON Encoding Extension adds support for model classes and datetimes
- MessagePack encoding is used to store rich type information when putting
  data into Redis, etc.
'''
from json import JSONEncoder
from datetime import date, time, datetime, timedelta
from decimal import Decimal
import msgpack
import re


class ModelJSONEncoder(JSONEncoder):
    '''
    Subclass of JSONEncoder that adds support for additional Python
    datatypes used in model objects
    '''

    def default(self, obj):
        # pylint: disable=method-hidden
        '''
        Use the default behavior unless the object is a datetime object
        (identified by the presence of the strftime attribute) or a model
        object (identified by the presence of a public_dict attribute)
        '''

        if isinstance(obj, Decimal):
            return '{0:f}'.format(obj)
        elif hasattr(obj, 'strftime'):
            if hasattr(obj, 'year') and hasattr(obj, 'hour'):
                format = '%Y-%m-%dT%H:%M:%SZ'
            elif hasattr(obj, 'year'):
                format = '%Y-%m-%d'
            else:
                format = '%H:%M:%S'

            return obj.strftime(format)
        elif hasattr(obj, 'total_seconds'):
            # obj looks like a timedelta
            total_s = int(obj.total_seconds())
            hours = (total_s // 3600)
            minutes = (total_s // 60) % 60
            seconds = total_s % 60
            return '{0:02}:{1:02}:{2:02}'.format(hours, minutes, seconds)
        elif hasattr(obj, 'public_dict'):
            return obj.public_dict
        else:
            return JSONEncoder.default(self, obj)


class MessagePackEncoder(object):
    '''
    Wrapper for MessagePack to build
    '''

    def _object_encode(self, obj):
        if isinstance(obj, Decimal):
            return {
                '__type__': 'decimal',
                '__repr__': str(obj)
            }
        elif hasattr(obj, 'strftime'):
            # datetime, date, and time all have a strftime method,
            # so we need to go deeper.
            if hasattr(obj, 'year') and hasattr(obj, 'hour'):
                # datetime has both year and hour
                return {
                    '__type__': 'datetime',
                    '__repr__': obj.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                }
            elif hasattr(obj, 'year'):
                # date has a year
                return {
                    '__type__': 'date',
                    '__repr__': obj.strftime('%Y-%m-%d')
                }
            else:
                # I guess time is all that's left
                return {
                    '__type__': 'time',
                    '__repr__': obj.strftime('%H:%M:%S.%fZ')
                }
        elif hasattr(obj, 'total_seconds'):
            # obj looks like a timedelta
            return {
                '__type__': 'timedelta',
                '__repr__': obj.total_seconds()
            }
        else:
            return obj

    def _object_decode(self, obj):
        type = obj.get('__type__', None)

        typemap = {
            'datetime': lambda x: datetime.strptime(x['__repr__'], "%Y-%m-%dT%H:%M:%S.%fZ"),
            'date': lambda x: date.strptime(x['__repr__'], "%Y-%m-%d"),
            'time': lambda x: time.strptime(x['__repr__'], "%H:%M:%S.%fZ"),
            'timedelta': lambda x: timedelta(seconds=x['__repr__']),
            'decimal': lambda x: Decimal(x['__repr__']),
            None: lambda x: x
        }

        return typemap[type](obj)

    def __init__(self):
        '''
        Create a new encoder instance.
        '''
        self.hooks = None

    def encode(self, obj):
        '''
        Encode an object as a MessagePack byte string
        '''
        return msgpack.packb(obj, use_bin_type=True, default=self._object_encode)

    def decode(self, str):
        '''
        Decode a MessagePack-encoded byte string into
        an instance of the appropriate type.
        '''
        kwargs = dict(
            encoding='utf-8',
            use_list=False,
            object_hook=self._object_decode
        )
        return msgpack.unpackb(str, **kwargs)

def urlify(unused_handler, string):
    '''Return a string that has been munged to remove URL-unfriendly
    characters. This is not the same as URL encoding.

    Steps:
        1. Replace spaces with hyphens
        2. Replace any non-alphanumeric character or allowed punctuation with
            the empty string. (Allowed punctuation includes hyphens, forward
            slashes, and periods)
        3. Remove periods or commas that preceed a slash or hyphen
        4. Transform the string to lower case
    '''
    string = re.sub(r'[^A-Za-z0-9-/.]', '', re.sub(r' +', '-', string))
    return re.sub(r'[.,]([/-])', r'\1', string).lower()
