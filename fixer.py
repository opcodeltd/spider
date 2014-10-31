# coding: utf-8

from __future__ import absolute_import

from furl import furl
import os
import json
from common import Common

import logging
logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

FETCH_SET = 'to_fetch'
SEEN_SET  = 'fetched'

class Fixer(Common):
    def run(self):
        for root, dirs, files in os.walk(self.database):
            for filename in files:
                if not filename.endswith('.data'):
                    continue

                with open(os.path.join(root, filename[:-5])) as fh:
                    data = json.load(fh)

                url = furl(data['url'])
                new_url = self.preprocess_url(url)

                if new_url and url == new_url:
                    continue

                # Create the new URL
                if new_url and not self.url_exists(new_url):
                    log.debug("Creating %s from %s" % (new_url, url))
                    with open(self.filename_for(data['url'], ext='data'), 'rb') as fh:
                        data['data'] = fh.read()
                    data['url'] = str(new_url)
                    self.url_write(new_url, data)

                    try:
                        url_filename = self.filename_for(data['url'], ext='urls')
                        urls = list(set([str(x) for x in self.extract_links(data)]))
                        with open(url_filename, 'w') as fh:
                            log.debug("Writing %s" % url_filename)
                            fh.write("\n".join(urls) + "\n")
                    except Exception as e:
                        log.error("Failed to parse URLs from %s: %s" % (filename, e))

                log.debug("Deleting URL %s" % url)
                self.url_delete(url)
