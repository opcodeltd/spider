# coding: utf-8

from __future__ import absolute_import

import requests
from furl import furl
from BeautifulSoup import BeautifulSoup
import os
import re
import json
from common import Common

import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class Fetcher(Common):
    def __init__(self, url, database):
        self.url = furl(url)
        self.database = database

        if not os.path.isdir(database):
            os.mkdir(database)

        config_filename = os.path.join(database, 'config.json')

        if os.path.isfile(config_filename):
            config = json.load(open(config_filename))
            if config['url'] != str(self.url):
                raise Exception("Wrong base URL for this database")
        else:
            with open(config_filename, 'w') as fh:
                json.dump(dict(
                    url = str(self.url)
                ), fh)

    def run(self):
        log.debug("Loading existing state")

        init_urls = set([self.url])

        for root, dirs, files in os.walk(self.database):
            for filename in files:
                if not filename.endswith('.data'):
                    continue
                with open(os.path.join(root, filename[:-5])) as fh:
                    data = json.load(fh)

                with open(self.filename_for(data['url'], data=True), 'rb') as fh:
                    data['data'] = fh.read()

                try:
                    init_urls.update(self.extract_links(data))
                except Exception as e:
                    log.error("Failed to parse URLs from %s: %s" % (data['url'], e))

        self.urls = list(init_urls)
        log.debug("Starting spider")
        self.spider()
        log.debug("Finished spider")

    def spider(self):
        import ipdb
        url = None
        with ipdb.launch_ipdb_on_exception():
            try:
                while len(self.urls):
                    url = self.urls.pop()

                    if self.url_exists(url):
                        # Already have this request stored
                        url = None
                        continue

                    response = self.request(url)

                    self.url_write(url, response)

                    try:
                        self.urls.extend(self.extract_links(response))
                    except Exception as e:
                        log.error("Failed to parse URLs from %s: %s" % (url, e))
                    url = None
            finally:
                if url:
                    self.url_delete(url)

    def request(self, url):
        log.debug('fetching: %s', url)
        res = requests.get(str(url), allow_redirects=False)
        return dict(
            url         = str(url),
            data        = res.content,
            status_code = res.status_code,
            headers     = dict(res.headers),
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
