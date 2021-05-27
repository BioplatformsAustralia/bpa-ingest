import tempfile
import shutil
import json
import os
from contextlib import suppress
from .libs.fetch_data import Fetcher, get_password, get_env_username


class DownloadMetadata:
    def __init__(
        self,
        logger,
        project_class,
        path=None,
        force_fetch=False,
        metadata_info=None,
        has_sql_context=False,
    ):
        self.cleanup = True
        self.fetch = True
        self._logger = logger
        self._set_path(path)
        self._set_auth(project_class)

        if metadata_info is None:
            metadata_info = {}

        sql_to_excel_context_classes = getattr(
            project_class, "sql_to_excel_context_classes", []
        )
        if has_sql_context == True and sql_to_excel_context_classes:
            contextual_classes = sql_to_excel_context_classes
        else:
            contextual_classes = getattr(project_class, "contextual_classes", [])

        self.contextual = [
            (os.path.join(self.path, c.name), c) for c in contextual_classes
        ]
        schema_classes = getattr(project_class, "schema_classes", [])
        self.schema_definitions = [
            (os.path.join(self.path, c.name), c) for c in schema_classes
        ]

        if self.fetch or force_fetch:
            self._fetch_metadata(project_class, self.contextual, metadata_info)

        self.project_class = project_class
        self.meta = self.make_meta(logger)

    def make_meta(self, logger):
        meta_kwargs = {}
        with open(self.info_json, "r") as fd:
            meta_kwargs["metadata_info"] = json.load(fd)
        if self.contextual:
            meta_kwargs["contextual_metadata"] = [
                c(self._logger, p) for (p, c) in self.contextual
            ]
        if self.schema_definitions:
            meta_kwargs["schema_definitions"] = [
                c(self._logger, p) for (p, c) in self.schema_definitions
            ]
        return self.project_class(logger, self.path, **meta_kwargs)

    def _fetch_metadata(self, project_class, contextual, metadata_info):
        for metadata_url in project_class.metadata_urls:
            self._logger.info(
                "fetching submission metadata: %s" % (project_class.metadata_urls)
            )
            fetcher = Fetcher(self._logger, self.path, metadata_url, self.auth)
            fetcher.fetch_metadata_from_folder(
                getattr(project_class, "metadata_patterns", None),
                metadata_info,
                getattr(project_class, "metadata_url_components", []),
            )

        with suppress(FileExistsError):
            os.mkdir(self.path)

        for contextual_path, contextual_cls in contextual:
            if not os.path.isdir(contextual_path):
                os.mkdir(contextual_path)
            else:
                self._logger.info(
                    "Context path: {} already exists. Moving on.".format(
                        contextual_path
                    )
                )
            self._logger.info(
                "fetching contextual metadata: %s" % (contextual_cls.metadata_urls)
            )
            for metadata_url in contextual_cls.metadata_urls:
                fetcher = Fetcher(
                    self._logger, contextual_path, metadata_url, self.auth
                )
                fetcher.fetch_metadata_from_folder(
                    getattr(contextual_cls, "metadata_patterns", None),
                    metadata_info,
                    getattr(contextual_cls, "metadata_url_components", []),
                )
        self.init_schema_classes(project_class, metadata_info)
        tmpf = self.info_json + ".new"
        with open(tmpf, "w") as fd:
            json.dump(metadata_info, fd)
        os.replace(tmpf, self.info_json)

    def init_schema_classes(self, project_class, metadata_info):
        if not self.schema_definitions:
            self._logger.info(
                f"No schema definitions exist for {getattr(project_class, 'ckan_data_type')}. Ignoring..."
            )
        for schema_path, schema_cls in self.schema_definitions:
            if not os.path.isdir(schema_path):
                os.mkdir(schema_path)
            else:
                self._logger.info(
                    "Metadata schema definitions path: {} already exists. Moving on.".format(
                        schema_path
                    )
                )
            self._logger.info(
                "fetching schema definitions metadata: %s" % (schema_cls.metadata_urls)
            )
            for metadata_url in schema_cls.metadata_urls:
                fetcher = Fetcher(self._logger, schema_path, metadata_url, self.auth)
                fetcher.fetch_metadata_from_folder(
                    getattr(schema_cls, "metadata_patterns", None),
                    metadata_info,
                    getattr(schema_cls, "metadata_url_components", []),
                )

    def _set_auth(self, project_class):
        env_auth_user = get_env_username()
        if env_auth_user is not None:
            self._logger.info(f"Using username from environment: {env_auth_user}")
            auth_user, auth_env_name = env_auth_user, env_auth_user
        else:
            self._logger.info(f"Defaulting to project auth...")
            auth_user, auth_env_name = project_class.auth
        self.auth = (auth_user, get_password(auth_env_name))

    def _set_path(self, path):
        # if we have a user-specified target directory, don't clean up at the end
        self.cleanup = path is None
        if path is None:
            path = tempfile.mkdtemp(prefix="bpaingest-metadata-")
        self.path = path
        self.info_json = os.path.join(path, "bpa-ingest.json")
        if os.access(self.info_json, os.R_OK):
            self._logger.info(
                "skipping metadata download, complete download in directory `%s' exists"
                % path
            )
            self.fetch = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cleanup:
            shutil.rmtree(self.path)
