# License comment goes here
#
# Written by Brendan Berg
# Copyright 2015, Brendan Berg

'''
Datastore Abstraction Classes

Currently just a database context manager. Futurely, will add connection
pools and a Redis context manager.
'''
# pylint: disable=star-args,abstract-class-not-used

from tornado.web import HTTPError
import MySQLdb

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
		self._settings = dict(settings, **settings.get(self._mode, {}))
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



