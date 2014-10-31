# coding: utf-8

from __future__ import absolute_import

from BeautifulSoup import BeautifulSoup
import hashlib
import os
import json
import logging
import re
from furl import furl
log = logging.getLogger(__name__)

class Common(object):
    def __init__(self, database):
        self.database = database

        config_filename = os.path.join(database, 'config.json')
        config = json.load(open(config_filename))
        self.url = furl(config['url'])

    def preprocess_url(self, url):
        url = furl(url)
        url.query.remove('sid')

        if url.query.params.get('file') == 'login':
            for key in url.query.params.keys():
                if key not in ['name', 'file']:
                    url.query.remove(key)

        if url.query.params.get('file') in ['posting', 'printview', 'privmsg']:
            return None

        return url

    def filename_for(self, url, mkdir=False, ext=None):
        filename = hashlib.md5(str(url)).hexdigest()
        #print " => %s %s" % (filename, str(url))
        dirname = filename[0:2]
        if mkdir and not os.path.isdir(os.path.join(self.database, dirname)):
            os.mkdir(os.path.join(self.database, dirname))

        fullpath = os.path.join(self.database, dirname, filename)

        if ext:
            return "%s.%s" % (fullpath, ext)

        return fullpath

    def url_exists(self, url):
        return os.path.isfile(self.filename_for(url))

    def url_write(self, url, content):
        with open(self.filename_for(url, mkdir=True, ext='data'), 'w') as fh:
            fh.write(content['data'])
        with open(self.filename_for(url, mkdir=True), 'w') as fh:
            json.dump(dict([(k, v) for k, v in content.items() if k != 'data']), fh)

    def url_read(self, url):
        with open(self.filename_for(url, mkdir=True), 'r') as fh:
            return json.load(fh)

    def url_delete(self, url):
        try:
            os.unlink(self.filename_for(url))
        except OSError:
            pass
        try:
            os.unlink(self.filename_for(url, ext='data'))
        except OSError:
            pass
        try:
            os.unlink(self.filename_for(url, ext='urls'))
        except OSError:
            pass

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
                        if e[attr].startswith('aim:'):
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
            url = self.preprocess_url(url)
            if url and self.url.scheme == url.scheme and self.url.host == url.host and str(url.path).startswith(str(self.url.path)):
                to_follow.append(url)

        return to_follow
