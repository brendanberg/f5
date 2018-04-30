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
JSON Encoding Extension adds support for model classes and datetimes
'''
from json import JSONEncoder


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

        if hasattr(obj, 'strftime'):
            return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif hasattr(obj, 'public_dict'):
            return obj.public_dict
        else:
            return JSONEncoder.default(self, obj)
