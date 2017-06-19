import abc
from .util import make_logger


logger = make_logger(__name__)


class ABCBaseMetadata(object):
    __metaclass__ = abc.ABCMeta
    # the package attribute we use to link resources to packages
    resource_linkage = ('bpa_id',)

    def _get_packages_and_resources(self):
        # ensure that each class can expect to have _get_packages() called first,
        # then _get_resources(), and only once in the entire lifetime of the class.
        if self._packages is None:
            self._packages = self._get_packages()
            self._resources = self._get_resources()
        return self._packages, self._resources

    def get_packages(self):
        self._get_packages_and_resources()
        return self._packages

    def get_resources(self):
        self._get_packages_and_resources()
        return self._resources

    @abc.abstractmethod
    def __init__(self, metadata_path):
        pass

    @abc.abstractmethod
    def _get_packages(self):
        """
        return a list of dictionaries representing CKAN packages
        private method, do not call directly.
        """
        pass

    @abc.abstractmethod
    def _get_resources(self):
        """
        return a list of tuples:
          (package_id, legacy_url, resource)
        private method, do not call directly.

        package_id:
          value of attr `resource_linkage` on the corresponding package for
          this resource
        legacy_url: link to download asset from legacy archive
        resource: dictionary representing CKAN resource
        """
        pass


class BaseMetadata(ABCBaseMetadata):
    def __init__(self):
        self._packages = self._resources = None
