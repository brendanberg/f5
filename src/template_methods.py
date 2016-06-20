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

"Helper functions for use in templates"

import re
from binascii import unhexlify
from base64 import urlsafe_b64encode as b64encode


def urlify(unused_handler, string):
	'''
	Return a string that has been munged to remove URL-unfriendly
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

def squish(unused_handler, obj_id):
	'''
	'''
	string = b64encode(unhexlify('%08x' % obj_id)).upper()
	return string.strip('=')

