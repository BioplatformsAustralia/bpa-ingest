import os
import pathlib
from urllib.parse import urljoin

from bpaingest.abstract import BaseMetadata
from bpaingest.libs.ingest_utils import from_comma_or_space_separated_to_list
from bpaingest.libs.raw_matcher import RawParser
from bpaingest.resource_metadata import (
    resource_metadata_from_file_no_data,
    resource_metadata_from_file,
)


class SecondaryMetadata(BaseMetadata):
    _raw_resources_file_basename = "raw_resources.json"

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
        self._raw_resources_linkage[linkage_key] = self.create_raw_resources_filename(
            linkage_key
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._raw_resources_linkage = {}

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

    def create_raw_resources_filename(self, linkages):
        sanitised_linkages = []
        for next_linkage in linkages:
            sanitised_linkages.append(next_linkage.split("/")[-1])
        tuple_for_filename = "_".join(sanitised_linkages)
        filename = f"{tuple_for_filename}_{self._raw_resources_file_basename}"
        path = os.path.join(self.path, filename)
        # // no need to open file yet, just create the name we will use later
        return path

    def generate_raw_resources(self):
        if len(self._raw_resources_linkage) == 0:
            self._logger.error("no raw resources, likely a bug in the ingest class")
        resources = []
        for linkage, fname in self._raw_resources_linkage.items():
            raw_resources_info = self.metadata_info.get(os.path.basename(fname), "")
            # if download_info exists for raw_resources, then use remote URL and gather all metadata including md5
            if raw_resources_info:
                resource = resource_metadata_from_file(
                    linkage, fname, self.ckan_data_type
                )
                legacy_url = urljoin(
                    raw_resources_info["base_url"], os.path.basename(fname)
                )
            else:
                # otherwise if no download_info, then use local URL and gather all metadata except md5
                resource = resource_metadata_from_file_no_data(
                    linkage, fname, self.ckan_data_type
                )
                legacy_url = pathlib.Path(os.path.abspath(fname)).as_uri()
            resources.append((linkage, legacy_url, resource))
        return resources

    def get_packages(self):
        self._get_packages_and_resources()
        return self._packages

    def get_resources(self):
        self._get_packages_and_resources()
        return self._resources
