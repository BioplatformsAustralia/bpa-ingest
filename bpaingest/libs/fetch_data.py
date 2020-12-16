# -*- coding: utf-8 -*-
"""
Utility functions to fetch data from web server
"""

import os
import re
import requests
from bs4 import BeautifulSoup
from distutils.dir_util import mkpath
from urllib.parse import urljoin

import requests.packages.urllib3

requests.packages.urllib3.disable_warnings()


class MissingCredentialsException(Exception):
    pass


class DownloadException(Exception):
    pass


def get_password(project_name=None):
    """Get downloads password for legacy auth username from environment """

    def complain_and_quit():
        raise MissingCredentialsException(
            "Please set shell variable {} to current BPA {} project password".format(
                password_env, project_name
            )
        )

    password_env = "BPA_%s_DOWNLOADS_PASSWORD" % (project_name.upper())
    if password_env is None:
        raise MissingCredentialsException("Set $%s" % (password_env))

    if password_env not in os.environ:
        complain_and_quit()

    password = os.environ[password_env]
    if password == "":
        complain_and_quit()

    return password


def get_env_username(username_variable="BPAINGEST_DOWNLOADS_USERNAME"):
    return os.getenv(username_variable)


class Fetcher:
    """ facilitates fetching data from webserver """

    recurse_re = re.compile(r"^[A-Za-z0-9_-]+/")

    def __init__(self, logger, target_folder, metadata_source_url, auth=None):
        self._logger = logger
        self.target_folder = target_folder
        self.metadata_source_url = metadata_source_url
        self.auth = auth
        self._ensure_target_folder_exists()

    def _ensure_target_folder_exists(self):
        if not os.path.exists(self.target_folder):
            mkpath(self.target_folder)

    def _fetch(self, session, base_url, name):
        self._logger.info("Fetching {} from {}".format(name, base_url))
        url = base_url + name
        with session.get(url, stream=True, auth=self.auth, verify=False) as r:
            if r.status_code != 200:
                raise DownloadException(
                    "status code {} for: {}".format(r.status_code, url)
                )
            output_file = self.target_folder + "/" + name
            with open(output_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
                        f.flush()

    def fetch_metadata_from_folder(
        self,
        metadata_patterns,
        metadata_info,
        url_components,
        download=True,
        _target_depth=-1,
        _url=None,
        _session=None,
    ):
        """
        walk a directory structure, grabbing files matching `metadata_patterns`.
        `url_components` gives an expected minimum level of recursing to find matching files,
        and the names in `url_components` are used to set `metadata_info` for each downloaded file.
        """

        if metadata_patterns is None:
            metadata_patterns = [r"^.*\.(md5|xlsx)$"]
        if _url is None:
            _url = self.metadata_source_url
        if _target_depth == -1:
            _target_depth = len(url_components)
        if _session is None:
            _session = requests.Session()
        self._logger.info("Fetching folder from {}".format(_url))
        response = _session.get(_url, stream=True, auth=self.auth, verify=False)
        if response.status_code != 200:
            self._logger.error(
                "warning: status code %d for url %s" % (response.status_code, _url)
            )
        fetched = set()
        for link in BeautifulSoup(response.content, "html.parser").find_all("a"):
            link_target = link.get("href")
            if link_target in fetched:
                continue
            fetched.add(link_target)
            # we need to descend directory tree further in order to find all `url_components`
            if _target_depth > 0:
                if Fetcher.recurse_re.match(link_target):
                    self.fetch_metadata_from_folder(
                        metadata_patterns,
                        metadata_info,
                        url_components,
                        download=download,
                        _session=_session,
                        _target_depth=_target_depth - 1,
                        _url=urljoin(_url, link_target),
                    )
            else:
                # descend anyway, to find whatever is there, but we've already hit target_depth
                if Fetcher.recurse_re.match(link_target):
                    self.fetch_metadata_from_folder(
                        metadata_patterns,
                        metadata_info,
                        url_components,
                        download=download,
                        _session=_session,
                        _target_depth=_target_depth,
                        _url=urljoin(_url, link_target),
                    )
                elif not any(
                    re.compile(pattern).match(link_target)
                    for pattern in metadata_patterns
                ):
                    continue
                else:
                    subdir = _url[len(self.metadata_source_url) :].strip("/")
                    meta_parts = subdir.split("/")[: len(url_components)]
                    assert len(meta_parts) == len(url_components)
                    if link_target in metadata_info:
                        raise DownloadException(
                            "Legacy archive contains non-unique filename: %s (%s)"
                            % (link_target, metadata_info[link_target])
                        )
                    metadata_info[link_target] = dict(
                        list(zip(url_components, meta_parts))
                    )
                    metadata_info[link_target]["base_url"] = _url
                    # download the actual file
                    if download:
                        self._fetch(_session, _url, link_target)
