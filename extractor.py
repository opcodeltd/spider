# coding: utf-8

from __future__ import absolute_import

from furl import furl
from BeautifulSoup import BeautifulSoup
import os
import json
from common import Common
from redis import StrictRedis
redis = StrictRedis()

import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

FETCH_SET = 'to_fetch'
SEEN_SET  = 'fetched'

class Extractor(Common):
    def __init__(self, database):
        self.database = database

        config_filename = os.path.join(database, 'config.json')
        config = json.load(open(config_filename))
        self.url = furl(config['url'])

    def run(self):
        redis.delete(FETCH_SET, SEEN_SET)
        for root, dirs, files in os.walk(self.database):
            for filename in files:
                if not filename.endswith('.data'):
                    continue

                url_filename = os.path.join(root, filename[:-5] + '.urls')

                with open(os.path.join(root, filename[:-5])) as fh:
                    data = json.load(fh)

                redis.sadd(SEEN_SET, data['url'])

                if os.path.isfile(url_filename):
                    with open(url_filename, 'r') as fh:
                        urls = [l.strip() for l in fh.readlines()]
                        if len(urls):
                            redis.sadd(FETCH_SET, *urls)
                else:
                    with open(self.filename_for(data['url'], data=True), 'rb') as fh:
                        data['data'] = fh.read()

                    try:
                        urls = list(set([str(x) for x in self.extract_links(data)]))
                    except Exception as e:
                        log.error("Failed to parse URLs from %s: %s" % (filename, e))
                        continue

                    if len(urls):
                        redis.sadd(FETCH_SET, *urls)
                    with open(url_filename, 'w') as fh:
                        log.debug("Writing %s" % url_filename)
                        fh.write("\n".join(urls) + "\n")
        redis.sdiffstore(FETCH_SET, FETCH_SET, SEEN_SET)
