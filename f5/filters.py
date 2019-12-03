from functools import wraps
from collections import Callable
from decimal import Decimal
import dateutil.parser
import datetime
import logging
import re


required = []
optional = []
default_date = datetime.datetime(datetime.MINYEAR, 1, 1)
# Shamelessly taken (and modified slightly) from here:
# http://codereview.stackexchange.com/a/19670
url_pattern = re.compile(
    r'^(?:http)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+' # domain...
    r'(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' # (cont'd)
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|' # ...or ipv4
    r'\[?[A-F0-9]*:[A-F0-9:]+\]?)' # ...or ipv6
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE)

# def validator(*args, **kwargs):
#     return validate_email.validate_email(*args, **kwargs)

def sanitized_dict(in_dict, filters, require_all=False):
    if require_all:
        return {k: get_sanitized(in_dict, k, v) for k, v in filters.items() if v is not optional}
    else:
        return {k: get_sanitized(in_dict, k, v) for k, v in filters.items() if k in in_dict}

def filter(input, filters):
    return {k: get_sanitized(input, k, v) for k, v in filters.items() if v is not optional}

def get_sanitized(in_dict, key, default=required):
    if default is required:
        return in_dict.get(key)
    elif isinstance(default, Callable):
        val = in_dict.get(key, None)
        return default(val)
    else:
        return in_dict.get(key, default)

def with_default(func, default=required):
    @wraps(func)
    def wrapped(value):
        if default is required and value is None:
            raise KeyError('required value is None')
        return func(value) if value else default
    return wrapped

def is_in_set(accepted_vals):
    logging.error(accepted_vals)
    def validate(value):
        logging.error(value)
        if value in accepted_vals:
            return value
        else:
            raise ValueError("invalid option value '{0}'".format(value))
    return validate

def iso_8601_datetime(value):
    #TODO [Brendan Berg]: Python dateutil parsing is some pretty insane
    # bullshit: https://gist.github.com/brendanberg/633b62c49bb20219ca1d
    # Probably find a better parser or write our own?
    return dateutil.parser.parse(value, default=default_date) if value else None

def url(value):
    if url_pattern.match(value):
        return value
    else:
        raise ValueError("invalid url string")

def email(value):
    '''Validate in_str as an email address.
    We rely on the validate_email module to parse the address. If the
    optional keyword argument 'check_mx' is true, the method also does a
    DNS lookup to make sure the domain has an SMTP server.
    '''
    raise NotImplementedError('depends on the validate_email module')
    # check_mx = kwargs.get('check_mx', False) is True
    # is_valid = validator(value, check_mx=check_mx)

    # if is_valid is True or is_valid is None:
    #     return value
    # else:
    #     raise ValueError('invalid email string')

def string(value):
    if not isinstance(value, str):
        raise ValueError('expected string')
    return value

def integer(value):
    if not isinstance(value, int):
        raise ValueError('expected integer')
    return value

def boolean(value):
    if not isinstance(value, bool):
        raise ValueError('expected boolean')
    return value

def float_or_int(value):
    if not isinstance(value, float) and not isinstance(value, int) and not isinstance(value, Decimal):
        raise ValueError('expected float or int')
    return value
