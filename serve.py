# coding: utf-8

from __future__ import absolute_import

from furl import furl
import os
from common import Common
from werkzeug.wrappers import Request, Response
import json

import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class Server(Common):
    def __init__(self, database):
        self.database = database

        config_filename = os.path.join(database, 'config.json')
        config = json.load(open(config_filename))
        self.url = furl(config['url'])

    def __call__(self, environ, start_response):
        request = Request(environ)
        lookup_url = self.url.copy().join(request.full_path)
        lookup_url = self.preprocess_url(lookup_url)

        if lookup_url and not self.url_exists(lookup_url) and request.full_path == '/':
            lookup_url = self.url.copy()

        if not lookup_url or not self.url_exists(lookup_url):
            log.debug('%d: %s' % (404, lookup_url))
            return Response('Not found', 404)(environ, start_response)

        data = self.url_read(lookup_url)

        data['headers'].pop('content-encoding', None)

        log.debug('%d: %s' % (data['status_code'], lookup_url))

        if data['status_code'] >= 300 and data['status_code'] < 400:
            url = furl(data['headers']['location'])
            url.scheme = request.scheme
            url.host = request.host
            url.port = None
            data['headers']['location'] = str(url)

        with open(self.filename_for(lookup_url, ext='data'), 'rb') as fh:
            content = fh.read()

        return Response(
            content,
            status = data['status_code'],
            headers = data['headers'].items(),
        )(environ, start_response)
