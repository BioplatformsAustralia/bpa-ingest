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
from urlparse import urljoin
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

    def fetch_metadata_from_folder(self, metadata_patterns, metadata_info, target_depth=0, url=None):
        """
        walk a directory structure. if `target_depth` == 0, then download files
        if `target_depth` > 0, recurse into subdirectories
        updates `metadata_info` if it is not `None` an entry for each metadata file
        downloaded
        """
        if metadata_patterns is None:
            metadata_patterns = [r'^.*\.(md5|xlsx)$']
        if url is None:
            url = self.metadata_source_url

        logger.info('Fetching folder from {}'.format(url))
        response = requests.get(url, stream=True, auth=self.auth, verify=False)
        recursive = target_depth > 0
        fetched = set()
        print(url, target_depth, metadata_patterns)
        for link in BeautifulSoup(response.content, 'html.parser').find_all('a'):
            link_target = link.get('href')
            if link_target in fetched:
                continue
            fetched.add(link_target)
            if recursive:
                if Fetcher.recurse_re.match(link_target):
                    print("RECURSE:", link_target)
                    self.fetch_metadata_from_folder(
                        metadata_patterns,
                        metadata_info,
                        target_depth=target_depth - 1,
                        url=urljoin(url, link_target))
            else:
                if not any(re.match(pattern, link_target) for pattern in metadata_patterns):
                    continue
                metadata_info[link_target] = url
                self._fetch(url, link_target)
