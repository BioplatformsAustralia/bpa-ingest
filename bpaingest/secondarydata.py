import os
import pathlib
import re
import tempfile
from urllib.parse import urljoin

from bpaingest.abstract import BaseMetadata
from bpaingest.libs.ingest_utils import from_comma_or_space_separated_to_list
from bpaingest.libs.raw_matcher import RawParser
from bpaingest.util import resource_metadata_from_file_no_data


class SecondaryMetadata(BaseMetadata):
    _raw_resources_file_name = "raw_resources.json"

    def parse_raw_list(self, lname):
        p = RawParser(lname, self.raw["match"], self.raw["skip"])
        for tpl in p.matches:
            yield tpl
        for tpl in p.no_match:
            self._logger.error("No match for filename: `%s'" % tpl)

    def _get_resources(self):
        raise NotImplementedError("implement _get_resources()")

    def _get_packages(self):
        raise NotImplementedError("implement _get_packages()")

    def _update_raw_resources(self):
        self._logger.info("Calculating raw resources...")
        for obj in self._packages:
            raw_resources = from_comma_or_space_separated_to_list(
                self._logger, obj["raw_resources"]
            )
            self._logger.info("have raw list: {}".format(raw_resources))
            raw_result = {}
            for filename, raw_info in self.parse_raw_list(raw_resources):
                raw_result[filename] = raw_info
            obj.update({"raw_resources": raw_result})
            self.track_raw_resources(obj)

    def track_raw_resources(self, obj):
        """
        track a spreadsheet that needs to be uploaded into the packages generated from it
        """

        linkage_key = tuple([obj[t] for t in self.resource_linkage])
        assert linkage_key not in self._raw_resources_linkage
        self._raw_resources_linkage[linkage_key] = self.create_raw_resources_file()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._raw_resources_linkage = {}

    def create_metadata_info_for_raw_resources(self):
        self._logger.info(f"got metadata info: {self.metadata_info}")
        res = [ k for k in self.metadata_info.keys() if k.endswith(".md5")]
        if not res:
            raise("Unable to find md5 file in metadata info.")
        dictionary_copy= self.metadata_info[res[0]].copy()
        self.metadata_info[self._raw_resources_file_name] = dictionary_copy

    def _get_packages_and_resources(self):
        # ensure that each class can expect to have _get_packages() called first,
        # then _get_resources(), and only once in the entire lifetime of the class.
        if self._packages is None:
            self._packages = self._get_packages()
            self._update_raw_resources()
            self._resources = self._get_resources()
            BaseMetadata.resources_add_format(self._resources)
            BaseMetadata.obj_round_floats_and_stringify(self._packages)
            BaseMetadata.obj_round_floats_and_stringify(
                t for _, _, t in self._resources
            )
        return self._packages, self._resources

    def create_raw_resources_file(self):
        # open an empty file in temp directory
        tmp = tempfile.mkdtemp()
        path = os.path.join(tmp, self._raw_resources_file_name)
        temp_file = open(path, "w+")
        return temp_file.name

    def generate_raw_resources(self):
        if len(self._raw_resources_linkage) == 0:
            self._logger.error("no raw resources, likely a bug in the ingest class")
        resources = []
        for linkage, fname in self._raw_resources_linkage.items():
            resource = resource_metadata_from_file_no_data(
                linkage, fname, self.ckan_data_type
            )
            legacy_url = pathlib.Path(fname).as_uri()
            resources.append((linkage, legacy_url, resource))
        return resources

    def get_packages(self):
        self._get_packages_and_resources()
        return self._packages

    def get_resources(self):
        self._get_packages_and_resources()
        return self._resources
