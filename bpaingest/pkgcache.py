
from .util import make_logger
from .ops import ckan_method


logger = make_logger(__name__)


def build_package_cache(ckan, ckan_data_type, sync_packages):
    """
    build a cache of all the packages in `org`, to speed up comparison.
    `sync_packages` is the packages we are aiming to set as our target
    state
    """
    package_types = set(t['type'] for t in sync_packages)
    package_types.add(ckan_data_type)
    packages = []
    for typ in package_types:
        logger.info("Retrieving all extant packages of type: {}".format(typ))
        results = ckan_method(ckan, 'package', 'search')(q='type:{}'.format(typ), include_private=True, rows=50000)
        packages += results['results']
    logger.info("{} packages cached.".format(len(packages)))
    return {t['id']: t for t in packages}


def build_resource_cache(*args):
    cache = {}
    for pkg in build_package_cache(*args).values():
        for resource in pkg['resources']:
            cache[resource['id']] = resource
    return cache
