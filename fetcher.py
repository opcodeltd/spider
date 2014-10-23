# coding: utf-8

from __future__ import absolute_import

import shelve
import requests
from furl import furl
from BeautifulSoup import BeautifulSoup
import os
import hashlib
import re

import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class Fetcher(object):
    def __init__(self, url, database):
        self.url = furl(url)
        self.database = database
        self.db = shelve.open(database)
        os.mkdir("%s-files" % database)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.db.close()

    def run(self):
        if 'config' in self.db:
            raise Exception("Database has already started spidering")

        self.db['config'] = dict(
            url = str(self.url)
        )
        self.urls = [self.url]
        self.spider()

    def spider(self):
        while len(self.urls):
            url = self.urls.pop()
            if str(url) in self.db:
                # Already got content for this URL
                continue

            response = self.request(url)
            self.db[str(url)] = dict([(k, v) for k, v in response.items() if k != 'data'])
            self.urls.extend(self.extract_links(response))

    def request(self, url):
        log.debug('fetching: %s', url)
        res = requests.get(str(url), allow_redirects=False)
        filename = self.store_file(url, res.content)
        return dict(
            filename    = filename,
            data        = res.text,
            status_code = res.status_code,
            headers     = res.headers,
        )

    def store_file(self, url, data):
        filename = self.filename_for(url)
        with open(filename, 'wb') as fh:
            fh.write(data)

        return filename

    def filename_for(self, url):
        return "%s-files/%s" % (
            self.database,
            hashlib.md5(str(url)).hexdigest(),
        )

    def extract_links(self, response):
        if response['status_code'] >= 300 and response['status_code'] < 400:
            if 'location' in response['headers']:
                return self.should_follow_filter(response['headers']['location'])

        if response['status_code'] == 200 and 'content-type' in response['headers']:
            if response['headers']['content-type'].startswith('text/html'):
                soup = BeautifulSoup(response['data'])
                links = []
                for attr in ['href', 'src']:
                    for e in soup.findAll(attrs = {attr: True}):
                        if e[attr].startswith('tel:'):
                            continue
                        links.append(e[attr])
                return self.should_follow_filter(*links)
            if response['headers']['content-type'].startswith('text/css'):
                return self.should_follow_filter(*re.findall(r'''url \( ["']? ([^)]+?) ["']? \)''', response['data'], flags=re.X))

        return []

    def should_follow_filter(self, *urls):
        to_follow = []
        for url in urls:
            url = self.url.copy().join(url).remove(fragment=True)
            if self.url.scheme == url.scheme and self.url.host == url.host and str(url.path).startswith(str(self.url.path)):
                to_follow.append(url)

        return to_follow
