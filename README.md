# Tornado + F5

__F5__ is a library of modules that provide commonly desired functionality when building RESTful web APIs and user-facing applications in [Tornado](http://tornadoweb.org/).

Features include model and service abstractions for data storage and retrieval; support for JSON, CORS, and other acronyms as well; and context managers for MySQL and Redis
connections.

## Module Overview

The __`models`__ module provides a base class for a minimal wrapper around database tables. Instances of a subclass of `Model` are initialized with a dictionary of column names and their associated values. The class provides dictionary-like column getters and setters and maintains a set of columns whose values have been modified since retrieval. The `public_dict` property allows programmers to customize the structure of the object returned to the end user.

The __`services`__ module provides an extendable base class for querying the datastore and returning model objects populated by rows in the result set. The `Service` class defines generic methods for retrieving model objects from a table, and retrieving objects related to a foreign model via a linking table. The base class also provides methods to insert, update, and delete models.

The __`handlers`__ module provides base classes for HTML and JSON request handlers. 

The __`storage`__ module provides simple context managers for database and key-value store connections. Support for connection pooling will be included in a future release.

The __`encoding`__ module provides the `ModelJSONEncoder` class, which adds automatic JSON encoding of model subclasses (via their `public_dict` property) and ISO-8601 encoding of `datetime` instances.

## License

This software is made available under the terms of the [MIT license](http://opensource.org/licenses/MIT).

Copyright &copy; 2015 The Electric Eye Company and Brendan Berg

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Except as contained in this notice, the name of a copyright holder shall not be used in advertising or otherwise to promote the sale, use or other dealings in this Software without prior written authorization of the copyright holder.
