import os
import re
from glob import glob
from urllib.parse import urljoin

from bpaingest.ops import ckan_method
from unipath import Path

from bpaingest.abstract import BaseMetadata
from bpaingest.libs import ingest_utils
from bpaingest.util import common_values, sample_id_to_ckan_name


class SecondaryMetadata(BaseMetadata):
    def _get_resources(self):
        raise NotImplementedError("implement _get_packages()")

    def _get_packages(self):
        raise NotImplementedError("implement _get_packages()")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_packages_and_resources(self):
        self._logger.debug("Inside secondary data...")
        # ensure that each class can expect to have _get_packages() called first,
        # then _get_resources(), and only once in the entire lifetime of the class.
        if self._packages is None:
            self._packages = self._get_packages()
            self._resources = self._get_resources()
            BaseMetadata.resources_add_format(self._resources)
            BaseMetadata.obj_round_floats_and_stringify(self._packages)
            BaseMetadata.obj_round_floats_and_stringify(
                t for _, _, t in self._resources
            )
        return self._packages, self._resources

    def get_raw_packages(self, ckan, obj):
        ckan_obj = ckan_method(ckan, "package", "show")(id=obj["name"])

    # def search_package_and_resources(self):
    #     # ckan api will only return first 1000 responses for some calls - so set very high limit.
    #     # Ensure that 'private' is turned on
    #     self._logger.info(f"Using org id: {self.__org_id}")
    #     # ticket = 'BPAOPS-10'
    #     ticket = 'BPAOPS-930'
    #     search_package_arguments = {
    #         "rows": 10000,
    #         "start": 0,
    #         # "fq": f"+owner_org:{self.__org_id} +ticket:{ticket} +comments:34222_1_18S_UNSW_CTCTCTAC_GCGTAAGA_AUGKE",
    #         "fq": f"+owner_org:{self.__org_id} +ticket:{ticket}",
    #         # "fq": f"+owner_org:{self.__org_id}",
    #         "facet.field": ["resources"],
    #         "include_private": True,
    #     }
    #     package_raw_results = self.__ckan.call_action(
    #         "package_search", search_package_arguments
    #     )

    def get_packages(self):
        self._get_packages_and_resources()
        return self._packages

    def get_resources(self):
        self._get_packages_and_resources()
        return self._resources
