# coding: utf-8

from __future__ import absolute_import

import shelve
import requests
from furl import furl
from BeautifulSoup import BeautifulSoup
import os
import hashlib
from werkzeug.wrappers import Request, Response

import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class Server(object):
    def __init__(self, database):
        self.database = database
        self.db = shelve.open(database)
        self.url = furl(self.db['config']['url'])

    def __call__(self, environ, start_response):
        request = Request(environ)
        lookup_url = self.url.copy().join(request.path)

        if str(lookup_url) not in self.db and request.path == '/':
            lookup_url = self.url.copy()

        if str(lookup_url) not in self.db:
            return Response('Not found', 404)(environ, start_response)

        data = self.db[str(lookup_url)]

        return Response(
            open(data['filename']),
            status = data['status_code'],
            headers = data['headers'].items(),
        )(environ, start_response)
