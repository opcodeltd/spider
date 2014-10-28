# coding: utf-8

from __future__ import absolute_import

import hashlib
import os
import json
import logging
log = logging.getLogger(__name__)

class Common(object):
    def filename_for(self, url, mkdir=False, data=False):
        filename = hashlib.md5(str(url)).hexdigest()
        dirname = filename[0:2]
        if mkdir and not os.path.isdir(os.path.join(self.database, dirname)):
            os.mkdir(os.path.join(self.database, dirname))

        fullpath = os.path.join(self.database, dirname, filename)
        if data:
            return "%s.data" % fullpath
        return fullpath

    def url_exists(self, url):
        return os.path.isfile(self.filename_for(url))

    def url_write(self, url, content):
        with open(self.filename_for(url, mkdir=True, data=True), 'w') as fh:
            fh.write(content['data'])
        with open(self.filename_for(url, mkdir=True), 'w') as fh:
            json.dump(dict([(k, v) for k, v in content.items() if k != 'data']), fh)

    def url_read(self, url):
        with open(self.filename_for(url, mkdir=True), 'r') as fh:
            return json.load(fh)

    def url_delete(self, url):
        log.warn("Deleting potentially partial state for %s" % url)
        try:
            os.unlink(self.filename_for(url))
        except OSError:
            pass
        try:
            os.unlink(self.filename_for(url, data=True))
        except OSError:
            pass
