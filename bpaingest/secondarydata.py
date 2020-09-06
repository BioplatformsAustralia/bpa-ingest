from bpaingest.abstract import BaseMetadata
from bpaingest.libs.raw_matcher import RawParser


class SecondaryMetadata(BaseMetadata):
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

    def _add_raw_resources(self):
        raise NotImplementedError("implement _get_raw_resources()")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_packages_and_resources(self):
        # ensure that each class can expect to have _get_packages() called first,
        # then _get_resources(), and only once in the entire lifetime of the class.
        if self._packages is None:
            self._packages = self._get_packages()
            self._add_raw_resources()
            self._resources = self._get_resources()
            BaseMetadata.resources_add_format(self._resources)
            BaseMetadata.obj_round_floats_and_stringify(self._packages)
            BaseMetadata.obj_round_floats_and_stringify(
                t for _, _, t in self._resources
            )
        return self._packages, self._resources

    def get_packages(self):
        self._get_packages_and_resources()
        return self._packages

    def get_resources(self):
        self._get_packages_and_resources()
        return self._resources
