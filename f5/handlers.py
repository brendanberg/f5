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
Make request handlers more useful
'''

from tornado.web import RequestHandler, HTTPError
from core.encoding import ModelJSONEncoder

import logging
import json


def authenticated(method):
	pass	

class HTMLRequestHandler(RequestHandler):
	"Adds initialization delegate to RequestHandler"
	# pylint: disable=abstract-method,too-many-public-methods

	def initialize(self, **kwargs):
		'''Overrides RequestHandler.initialize and calls the
		self.initialize_delegate method if there is one'''
		if hasattr(self, 'initialize_delegate'):
			# pylint: disable=no-member
			# I mean come on, we explicitly checked for it!
			self.initialize_delegate(**kwargs)


class JSONRequestHandler(RequestHandler):
	"Adds methods for rendering JSON responses to the client"
	# pylint: disable=abstract-method,too-many-public-methods

	def __init__(self):
		super(JSONRequestHandler, self).__init__()
		self._jsonp_pattern = None
		self._jsonp_callback = None

	def _jsonp_callback_sanitize(self, callback_string):
		"Return callback_string if 
		if not self._jsonp_pattern:
			import re
			self._jsonp_pattern = re.compile(r'[a-zA-Z][a-zA-Z0-9_]{,50}')

		match = self._jsonp_pattern.match(callback_string)

		if match:
			return callback_string
		else:
			raise ValueError('__')

	def initialize(self, **kwargs):
		callback = None

		if self.get_argument('jsonp', None):
			callback = str(self.get_argument('jsonp'))
		elif self.get_argument('callback', None):
			callback = str(self.get_argument('callback'))

		if callback:
			try:
				callback = self._jsonp_callback_sanitize(callback)
			except ValueError:
				raise HTTPError(400, 'invalid callback')

			self._jsonp_callback = callback

		if self.application.configuration['tornado'].get('debug', False) == True:
			logging.info(self.request.arguments)

		if hasattr(self, 'initialize_delegate'):
			# pylint: disable=no-member
			# I mean come on, we explicitly checked for it!
			self.initialize_delegate(**kwargs)

	def get_current_user(self):
		logging.error(
			'attempt to authenticate in %s without an auth mixin in class lookup path\n%s',
			self.__class__.__name__, tb
		)
		return None

	def write_json(self, obj):
		"Writes the JSON-stringified value of obj to the response stream"

		if self._jsonp_callback:
			self.set_header('Content-Type', 'application/javascript')
		else:
			self.set_header('Content-Type', 'application/json; charset=UTF-8')

		# TODO: Cache management using Etag and If-None-Match headers

		response = json.dumps(obj, cls=ModelJSONEncoder)

		if self._jsonp_callback:
			response = '/*_*/{0}({1});'.format(self._jsonp_callback, response)

		self.write(response)
