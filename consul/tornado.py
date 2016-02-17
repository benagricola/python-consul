from __future__ import absolute_import

from six.moves import urllib

from tornado import httpclient
from tornado import gen

from consul import base


__all__ = ['Consul']


class HTTPClient(object):
    def __init__(self, host='127.0.0.1', port=8500, scheme='http',
                 verify=True, timeout=None):
        self.host = host
        self.port = port
        self.scheme = scheme
        self.verify = verify
        self.base_uri = '%s://%s:%s' % (self.scheme, self.host, self.port)
        self.client = httpclient.AsyncHTTPClient()
        self.timeout = timeout

    def uri(self, path, params=None):
        uri = self.base_uri + path
        if not params:
            return uri
        return '%s?%s' % (uri, urllib.parse.urlencode(params))

    def response(self, response):
        return base.Response(
            response.code, response.headers, response.body.decode('utf-8'))

    @gen.coroutine
    def _request(self, callback, request, timeout=None):
        try:
            # If request is a string rather than HTTPRequest, inject
            # timeout params
            if not isinstance(request, httpclient.HTTPRequest):
                timeout = timeout if timeout else self.timeout
                response = yield self.client.fetch(
                    request,
                    connect_timeout=timeout,
                    request_timeout=timeout)
            else:
                response = yield self.client.fetch(request)
        except httpclient.HTTPError as e:
            if e.code == 599:
                raise base.Timeout
            response = e.response
        raise gen.Return(callback(self.response(response)))

    def get(self, callback, path, params=None, timeout=None):
        uri = self.uri(path, params)
        return self._request(callback, uri, timeout)

    def put(self, callback, path, params=None, data='', timeout=None):
        uri = self.uri(path, params)
        timeout = timeout if timeout else self.timeout
        request = httpclient.HTTPRequest(uri, method='PUT', body=data,
                                         validate_cert=self.verify,
                                         connect_timeout=timeout,
                                         request_timeout=timeout)
        return self._request(callback, request)

    def delete(self, callback, path, params=None, timeout=None):
        uri = self.uri(path, params)
        timeout = timeout if timeout else self.timeout
        request = httpclient.HTTPRequest(uri, method='DELETE',
                                         validate_cert=self.verify,
                                         connect_timeout=timeout,
                                         request_timeout=timeout)
        return self._request(callback, request)


class Consul(base.Consul):
    def connect(self, host, port, scheme, verify=True, timeout=None):
        return HTTPClient(host, port, scheme, verify=verify, timeout=timeout)
