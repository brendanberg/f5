'''
Tests for basic HTTP request handling
'''

from unittest import TestCase
from urllib import parse

from tornado.web import Application
from tornado.httputil import HTTPHeaders
from tornado.httputil import HTTPConnection
from tornado.httputil import HTTPServerRequest

from f5.handlers import BaseRequestHandler


class TestBuildURL(TestCase):

    def setUp(self):
        def set_close_callback(_, *args, **kwargs):
            return None

        app = Application()
        app.configuration = {
                'tornado': {'debug': True}
            }
        
        conn = HTTPConnection()
        conn.set_close_callback = set_close_callback

        self.app = app
        self.conn = conn
        self.protocol = 'https'
        self.host = 'www.example.com'
        self.prefix = '/a'

    def get_handler(self, protocol=None, host=None, prefix=None):
        req = HTTPServerRequest(method='GET', uri='/',
            headers=HTTPHeaders({'X-Path-Prefix': prefix or self.prefix})
        )
        req.protocol = protocol or self.protocol
        req.host = host or self.host
        req.connection = self.conn

        return BaseRequestHandler(self.app, req)

    
    def test_emtpy_string(self):
        '''
        build_url returns <PROTOCOL>://<HOST><PREFIX>
        '''
        handler = self.get_handler()

        self.assertEqual(handler.build_url(''), '{0}://{1}{2}'.format(
            self.protocol, self.host, self.prefix))

    def test_path_prefix(self):
        '''
        build_url returns <PROTOCOL>://<HOST><PREFIX>
        '''
        base_url = '{0}://{1}'.format(self.protocol, self.host)

        handler = self.get_handler(prefix='/apple')
        self.assertEqual(handler.build_url(''), base_url + '/apple')

        handler = self.get_handler(prefix='/banana/')
        self.assertEqual(handler.build_url(''), base_url + '/banana')

        handler = self.get_handler(prefix='cherry/')
        self.assertEqual(handler.build_url(''), base_url + '/cherry')

    def test_path_parameters(self):
        '''
        build_url constructs parameterized paths
        '''
        base_url = '{0}://{1}{2}'.format(self.protocol, self.host, self.prefix)
        handler = self.get_handler()

        self.assertEqual(handler.build_url('/item/{0}', ['123']), base_url + '/item/123')
        self.assertEqual(handler.build_url('/item/{id}', {'id': '123'}), base_url + '/item/123')
        self.assertEqual(handler.build_url('/item/{0}/more', [123]), base_url + '/item/123/more')

    def test_query_arguments(self):
        '''
        build_url constructs querystring arguments from a dictionary
        '''
        handler = self.get_handler()

        def get_query_args(url):
            query_str = parse.urlparse(url).query
            return parse.parse_qs(query_str) if query_str else {}

        url = handler.build_url('/', query={'a': 'apple'})
        self.assertEqual(get_query_args(url), {'a': ['apple']})

        url = handler.build_url('/', query={'a': 'apple', 'b': 'banana'})
        self.assertEqual(get_query_args(url), {'a': ['apple'], 'b': ['banana']})

