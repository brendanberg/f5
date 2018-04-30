# Written by Brendan Berg
# Copyright 2015, Electric Eye Company

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
