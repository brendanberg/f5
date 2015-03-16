# Written by Brendan Berg
# Copyright 2015, The Electric Eye Company

'''
JSON Encoding Extension adds support for model classes and datetimes
'''
from json import JSONEncoder


class ModelJSONEncoder(JSONEncoder):
	''' Subclass of JSONEncoder that adds support for additional Python
		datatypes used in model objects'''

	def default(self, obj):
		# pylint: disable=method-hidden
		''' Use the default behavior unless the object is a datetime object
			(identified by the presence of the strftime attribute) or a model
			object (identified by the presence of a public_dict attribute)'''

		if hasattr(obj, 'strftime'):
			return obj.strftime('%Y-%m-%dT%H:%M:%SZ')
		elif hasattr(obj, 'public_dict'):
			return obj.public_dict
		else:
			return JSONEncoder.default(self, obj)

