# coding: utf-8

from __future__ import absolute_import

import requests
from furl import furl
import os
import json
from common import Common
from redis import StrictRedis
redis = StrictRedis()

FETCH_SET = 'to_fetch'
SEEN_SET  = 'fetched'

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
        log.debug("Starting spider")
        self.spider()
        log.debug("Finished spider")

    def spider(self):
        import ipdb
        url = None
        with ipdb.launch_ipdb_on_exception():
            try:
                url = furl(redis.spop(FETCH_SET))
                while url:
                    url = self.preprocess_url(url)

                    if redis.sismember(SEEN_SET, str(url)):
                        # Already have this request stored
                        url = None
                        continue

                    response = self.request(url)

                    self.url_write(url, response)
                    redis.sadd(SEEN_SET, str(url))

                    try:
                        urls = set([str(x) for x in self.extract_links(response)])
                        urls = [x for x in urls if not redis.sismember(SEEN_SET, str(x))]
                        if len(urls):
                            redis.sadd(FETCH_SET, *urls)
                    except Exception as e:
                        log.error("Failed to parse URLs from %s: %s" % (url, e))

                    url = furl(redis.spop(FETCH_SET))

                url = None
            finally:
                if url:
                    redis.sadd(FETCH_SET, str(url))
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
