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
import functools

try:
	from urllib import parse
except ImportError:
	import urlparse as parse

import logging
import json
import re


def authenticated(unused_method):
	"Decorator for verifying authentication on handlers"
	pass


class OriginMixin(object):
	ALLOWED_ORIGINS = []


def cross_origin(wrapped):
	@functools.wraps(wrapped)
	def wrapper(self, *args, **kwargs):
		origin = self.request.headers.get('Origin', None)

		if not origin:
			return

		pattern = getattr(self, '_origin_pattern', None)

		logging.info('[Cross-Origin] request origin: %s', origin)

		if not pattern:
			pattern = re.compile(r'https?://([^/]+)/?')
			self._origin_pattern = pattern

		match = pattern.match(origin)
		
		if match:
			host_origin = match.groups()[0]
			allowed_origin = False

			for rexp in self.ALLOWED_ORIGINS:
				# TODO: Cache the compiled exprs
				r = re.compile(rexp)
				if r.match(host_origin):
					allowed_origin = True
					break

			if not allowed_origin:
				logging.info('[Cross-Origin] Host %s is not allowed', host_origin)
				self.set_status(405)
				return

			
			logging.info('[Cross-Origin] Generating headers for host %s', host_origin)

			method_list = self.get_implemented_methods()

			self.set_header('Access-Control-Allow-Origin', origin)
			self.set_header('Access-Control-Allow-Credentials', 'true')
			self.set_header('Access-Control-Allow-Methods', ', '.join(method_list))
			self.set_header('Access-Control-Allow-Headers', (
				'Origin, Content-Type, Accept, Accept-Encoding, ' +
				'If-Modified-Since, Cookie, X-Xsrftoken, Etag'))

		return wrapped(self, *args, **kwargs)
	return wrapper


class BaseRequestHandler(RequestHandler):
	"Adds initialization delegate to RequestHandler"
	# pylint: disable=abstract-method,too-many-public-methods

	ALLOWED_ORIGINS = []

	def get_implemented_methods(self):
		method_list = ['OPTIONS']

		# We look up each supported HTTP method in both the base class
		# and the subclass, and compare their function objects. If they
		# don't match, the subclass must have defined an overriding
		# method, so we add the method to the allowed methods header.

		for method in self.SUPPORTED_METHODS:
			# N.B. We're relying on the SUPPORTED_METHODS property
			# defined in the tornado.web.RequestHandler class.
			override = getattr(self, method.lower())
			baseImpl = getattr(BaseRequestHandler, method.lower())

			if override.__func__ is not baseImpl.__func__:
				method_list.append('{0}'.format(method))

		return method_list

	@cross_origin
	def options(self):
		self.set_header('Allow', ','.join(self.get_implemented_methods()))
		# self.write(self.get_implemented_methods())

	def initialize(self, **kwargs):
		'''Overrides RequestHandler.initialize and calls the
		self.initialize_delegate method if there is one'''
		arguments = parse.parse_qs(self.request.query)
		self.request.utf_query_arguments = arguments
		# TODO: FIXME: This is a weird workaround to get scalar values out of
		# the arguments dictionary for use in the schematics validators
		self.request.utf_query_argitems = {
				k: v[0] for k, v in arguments.items()
		}

		config = self.application.configuration
		if config['tornado'].get('debug', False) == True:
			logging.info(self.request.utf_query_arguments)

		self.environment = config['tornado'].get('environment', 'development')
		self.data_service = kwargs.get('data_service')
		self.services = kwargs.get('services', {})

		if hasattr(self, 'initialize_delegate'):
			# pylint: disable=no-member
			# We explicitly checked for it!
			self.initialize_delegate(**kwargs)

	def prepare(self):
		"Prepare the request"
		content_type = self.request.headers.get('Content-Type')
		json_types = [
			'application/json; charset=utf-8',
			'application/json',
		]

		if content_type in json_types:
			logging.info(self.request.body.decode('utf-8'))
			self.json_body = json.loads(self.request.body.decode('utf-8'))

	def get_environment(self):
		return getattr(self, 'environment', 'development')

	def get_current_user(self):
		logging.info(
			'attempt to authenticate in %s without an auth mixin in class lookup path',
			self.__class__.__name__
		)
		return None


class HTMLRequestHandler(BaseRequestHandler):
	"Adds initialization delegate to RequestHandler"
	# pylint: disable=abstract-method,too-many-public-methods
	pass


class JSONRequestHandler(BaseRequestHandler):
	"Adds methods for rendering JSON responses to the client"
	# pylint: disable=abstract-method,too-many-public-methods

	def __init__(self, *args, **kwargs):
		super(JSONRequestHandler, self).__init__(*args, **kwargs)
		self._jsonp_pattern = None
		self._jsonp_callback = None

	def _jsonp_callback_sanitize(self, callback_string):
		"Return callback_string if it is a valid JavaScript identifier"
		if not self._jsonp_pattern:
			import re
			self._jsonp_pattern = re.compile(r'[a-zA-Z][a-zA-Z0-9_]{,50}')

		match = self._jsonp_pattern.match(callback_string)

		if match:
			return callback_string
		else:
			raise ValueError('__')

	def initialize(self, **kwargs):
		super(JSONRequestHandler, self).initialize(**kwargs)
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
			logging.info(self.request.utf_query_arguments)

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

