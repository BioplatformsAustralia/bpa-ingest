import abc


class BaseMetadata(object):
    __metaclass__ = abc.ABCMeta
    # the package attribute we use to link resources to packages
    resource_linkage = 'bpa_id'

    @abc.abstractmethod
    def __init__(self, metadata_path):
        pass

    @abc.abstractmethod
    def get_packages(self):
        """
        return a list of dictionaries representing CKAN packages
        """
        pass

    @abc.abstractmethod
    def get_resources(self):
        """
        return a list of tuples:
          (package_id, legacy_url, resource)

        package_id:
          value of attr `resource_linkage` on the corresponding package for
          this resource
        legacy_url: link to download asset from legacy archive
        resource: dictionary representing CKAN resource
        """
        pass
