import abc


class BaseMetadata(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def __init__(self, metadata_path):
        pass

    @abc.abstractmethod
    def get_group(self):
        pass

    @abc.abstractmethod
    def get_packages(self):
        pass

    @abc.abstractmethod
    def get_resources(self):
        pass
