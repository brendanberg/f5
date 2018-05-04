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
Multiple dispatch implementation for rendering models based on output destination
'''

from enum import Enum


def multimethod(cls, *types):
    '''
    Multiple dispatch decorator
    '''
    def wrapper(function):
        '''
        Wrapper function
        '''
        function = getattr(function, '__lastreg__', function)
        name = function.__name__
        dispatcher = getattr(cls, name, None)

        if dispatcher is None:
            dispatcher = MultiMethod(name)
            setattr(cls, name, dispatcher)

        dispatcher.register(types, function)
        dispatcher.__lastreg__ = function
        return dispatcher
    return wrapper


class MultiMethod(object):
    '''
    The MultiMethod class encapsulates the multiple implementations of a
    named method and the dispatch rules to select the correct implementation

    The `__get__` method contains the dispatch logic to select the correct
    method implementation to call
    '''
    # pylint: disable=too-few-public-methods

    def __init__(self, name):
        self.name = name
        self.typemap = {}

    def __get__(self, obj, owner=None):
        def dispatch(model, specifier, **kwargs):
            '''
            Intercept the arguments to the multimethod call and select
            the appropriate implementation
            '''
            # This is where the lookup magic happens. If we wanted
            # to generalize the dispatch rules, we could hook in here.
            # build_tuple(...) -> tuple
            # MultiMethod.lookup(tuple) -> function
            types = (model.__class__, specifier)
            function = self.typemap.get(types)

            if function is None:
                name = None

                raise TypeError(
                    'no matching function implementation for (%s, %s)' %
                    (model.__class__.__name__, specifier))

            return function(obj, model, specifier, **kwargs)

        if obj is None:
            return self
        else:
            return dispatch

    def register(self, types, function):
        '''
        Associate a function implementation with a type tuple to dispatch on

        Arguments:
            types: a tuple of hashable objects to match against
            function: the implementation to call when the multimethod is called
                with values whose types match the `types` tuple
        '''
        if types in self.typemap:
            raise TypeError('duplicate registration')

        self.typemap[types] = function
