import urllib.parse
import re
import os

from .ops import ckan_method
from .util import make_logger
from .libs.multihash import generate_hashes
from .pkgcache import build_resource_cache

logger = make_logger(__name__)


def localpath(mirror_path, legacy_url):
    path = urllib.parse.urlparse(legacy_url).path
    if path.startswith('/bpa/'):
        path = path[5:]
    path = path.lstrip('/')
    return os.path.join(mirror_path, path)


size_re = re.compile(r'^[0-9]+$')


def size_valid(resource):
    size = resource.get('size', '')
    return size is not None and size_re.match(size)


def is_hashed(resource):
    return size_valid(resource) and resource.get('s3etag_33554432')


def calculate_hashes(ckan, mirror_path, legacy_url, resource):
    fpath = localpath(mirror_path, legacy_url)
    patch_obj = {}
    resource_path = 'dataset/%s/resource/%s' % (resource['package_id'], resource['id'])

    if not size_valid(resource):
        patch_obj['size'] = str(os.stat(fpath).st_size)

    if not resource.get('s3etag_33554432'):
        hashes = generate_hashes(fpath)
        if hashes['md5'] != resource['md5']:
            logger.critical(
                "MD5 hash mismatch of on-disk data. Have `{}' and expected `{}': {}".format(
                    hashes['md5'], resource['md5'], fpath))
            return
        patch_obj.update(hashes)

    if not patch_obj:
        return

    patch_obj['id'] = resource['id']
    ckan_method(ckan, 'resource', 'patch')(**patch_obj)
    logger.info("%s: hashes calculated and pushed" % (resource_path))


def genhash(ckan, meta, mirror_path, num_threads):
    cache = build_resource_cache(ckan, meta.ckan_data_type, meta.get_packages())
    logger.info("%d resources of type %s" % (len(meta.get_resources()), meta.ckan_data_type))

    queue = []
    for _, legacy_url, target_resource in meta.get_resources():
        resource_id = target_resource['id']
        resource = cache.get(resource_id)
        if resource is None:
            logger.error("%s: not in CKAN, skipping" % (resource_id))
            continue
        if not is_hashed(resource):
            queue.append((legacy_url, resource))

    logger.info("{} resources to be hashed".format(len(queue)))
    for task in queue:
        calculate_hashes(ckan, mirror_path, *task)
