import tempfile
import shutil
import json
import os
from contextlib import suppress
from .util import make_logger
from .libs.fetch_data import Fetcher, get_password


logger = make_logger(__name__)


class DownloadMetadata(object):
    def __init__(self, project_class, path=None, force_fetch=False, metadata_info=None):
        self.cleanup = True
        self.fetch = True
        self._set_path(path)
        self._set_auth(project_class)

        if metadata_info is None:
            metadata_info = {}

        contextual_classes = getattr(project_class, "contextual_classes", [])
        self.contextual = [
            (os.path.join(self.path, c.name), c) for c in contextual_classes
        ]

        if self.fetch or force_fetch:
            self._fetch_metadata(project_class, self.contextual, metadata_info)

        self.project_class = project_class
        self.meta = self.make_meta()

    def make_meta(self):
        meta_kwargs = {}
        with open(self.info_json, "r") as fd:
            meta_kwargs["metadata_info"] = json.load(fd)
        if self.contextual:
            meta_kwargs["contextual_metadata"] = [c(p) for (p, c) in self.contextual]
        return self.project_class(self.path, **meta_kwargs)

    def _fetch_metadata(self, project_class, contextual, metadata_info):
        for metadata_url in project_class.metadata_urls:
            logger.info(
                "fetching submission metadata: %s" % (project_class.metadata_urls)
            )
            fetcher = Fetcher(self.path, metadata_url, self.auth)
            fetcher.fetch_metadata_from_folder(
                getattr(project_class, "metadata_patterns", None),
                metadata_info,
                getattr(project_class, "metadata_url_components", []),
            )

        with suppress(FileExistsError):
            os.mkdir(self.path)

        for contextual_path, contextual_cls in contextual:
            os.mkdir(contextual_path)
            logger.info(
                "fetching contextual metadata: %s" % (contextual_cls.metadata_urls)
            )
            for metadata_url in contextual_cls.metadata_urls:
                fetcher = Fetcher(contextual_path, metadata_url, self.auth)
                fetcher.fetch_metadata_from_folder(
                    getattr(contextual_cls, "metadata_patterns", None),
                    metadata_info,
                    getattr(contextual_cls, "metadata_url_components", []),
                )
        tmpf = self.info_json + ".new"
        with open(tmpf, "w") as fd:
            json.dump(metadata_info, fd)
        os.replace(tmpf, self.info_json)

    def _set_auth(self, project_class):
        auth_user, auth_env_name = project_class.auth
        self.auth = (auth_user, get_password(auth_env_name))

    def _set_path(self, path):
        self.info_json = os.path.join(path, "bpa-ingest.json")
        if path is not None:
            self.path = path
            self.cleanup = False
            if os.access(self.info_json, os.R_OK):
                logger.info(
                    "skipping metadata download, complete download in directory `%s' exists" % path
                )
                self.fetch = False
        else:
            self.path = tempfile.mkdtemp(prefix="bpaingest-metadata-")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cleanup:
            shutil.rmtree(self.path)
