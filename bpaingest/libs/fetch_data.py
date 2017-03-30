# -*- coding: utf-8 -*-
'''
Utility functions to fetch data from web server
'''

import os
import re
import sys
import requests
from bs4 import BeautifulSoup
from distutils.dir_util import mkpath
from urlparse import urljoin, urlsplit
from ..util import make_logger

import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

logger = make_logger(__name__)


def get_password(project_name=None):
    '''Get downloads password for project from environment '''

    def complain_and_quit():
        logger.error('Please set shell variable {} to current BPA {} project password'.format(password_env,
                                                                                              project_name))
        sys.exit()

    password_env = 'BPA_%s_DOWNLOADS_PASSWORD' % (project_name.upper())
    if password_env is None:
        logger.error('Set $%s' % (password_env))
        sys.exit()

    if password_env not in os.environ:
        complain_and_quit()

    password = os.environ[password_env]
    if password == '':
        complain_and_quit()

    return password


class Fetcher():
    ''' facilitates fetching data from webserver '''
    recurse_re = re.compile(r'^[A-Za-z0-9_-]+/')

    def __init__(self, target_folder, metadata_source_url, auth=None):
        self.target_folder = target_folder
        self.metadata_source_url = metadata_source_url
        self.auth = auth
        self._ensure_target_folder_exists()

    def _ensure_target_folder_exists(self):
        if not os.path.exists(self.target_folder):
            mkpath(self.target_folder)

    def _fetch(self, url, name):
        logger.info('Fetching {0} from {1}'.format(name, url))
        r = requests.get(url + name, stream=True, auth=self.auth, verify=False)
        with open(self.target_folder + '/' + name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()

    def fetch_metadata_from_folder(self, metadata_patterns, metadata_info, url_components, _target_depth=-1, _url=None):
        """
        walk a directory structure. if `target_depth` == 0, then download files
        if `target_depth` > 0, recurse into subdirectories
        updates `metadata_info` if it is not `None` an entry for each metadata file
        downloaded
        """
        if metadata_patterns is None:
            metadata_patterns = [r'^.*\.(md5|xlsx)$']
        if _url is None:
            _url = self.metadata_source_url
        if _target_depth == -1:
            _target_depth = len(url_components)
        # def _metadata_fn(url):
        #    return urlsplit(url).path.strip('/').split('/')[-target_depth:]

        logger.info('Fetching folder from {}'.format(_url))
        response = requests.get(_url, stream=True, auth=self.auth, verify=False)
        recursive = _target_depth > 0
        fetched = set()
        for link in BeautifulSoup(response.content, 'html.parser').find_all('a'):
            link_target = link.get('href')
            if link_target in fetched:
                continue
            fetched.add(link_target)
            if recursive:
                if Fetcher.recurse_re.match(link_target):
                    self.fetch_metadata_from_folder(
                        metadata_patterns,
                        metadata_info,
                        url_components,
                        _target_depth=_target_depth - 1,
                        _url=urljoin(_url, link_target))
            else:
                if not any(re.match(pattern, link_target) for pattern in metadata_patterns):
                    continue
                meta_parts = urlsplit(_url).path.strip('/').split('/')[-len(url_components):]
                metadata_info[link_target] = dict(zip(url_components, meta_parts))
                self._fetch(_url, link_target)
