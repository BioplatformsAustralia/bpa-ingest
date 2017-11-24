import urllib.parse
import ckanapi
import re
import os

from threading import Thread
from queue import Queue
from .ops import ckan_method
from .util import make_logger
from .libs.multihash import generate_hashes

logger = make_logger(__name__)


def localpath(mirror_path, legacy_url):
    path = urllib.parse.urlparse(legacy_url).path
    if path.startswith('/bpa/'):
        path = path[5:]
    path = path.lstrip('/')
    return os.path.join(mirror_path, path)


size_re = re.compile(r'^[0-9]+$')


def genhash(ckan, meta, mirror_path, num_threads):
    def calculate_hashes(bpa_id, legacy_url, resource):
        fpath = localpath(mirror_path, legacy_url)
        patch_obj = {}

        try:
            ckan_resource = ckan_method(ckan, 'resource', 'show')(id=resource['id'])
        except ckanapi.errors.NotFound:
            logger.error("%s: not in CKAN, skipping" % (resource['id']))
            return
        resource_path = 'dataset/%s/resource/%s' % (ckan_resource['package_id'], ckan_resource['id'])

        size = ckan_resource.get('size', '')
        if size is None or not size_re.match(size):
            patch_obj['size'] = str(os.stat(fpath).st_size)

        if not ckan_resource.get('s3etag_33554432'):
            hashes = generate_hashes(fpath)
            if hashes['md5'] != resource['md5']:
                logger.critical("MD5 hash mismatch of on-disk data. Have `%s' and expected `%s': %s" % (hashes['md5'], resource['md5'], fpath))
                return
            patch_obj.update(hashes)

        if not patch_obj:
            logger.info("%s: already hashed, continuing" % (resource_path))
            return

        patch_obj['id'] = ckan_resource['id']
        ckan_method(ckan, 'resource', 'patch')(**patch_obj)
        logger.info("%s: hashes calculated and pushed" % (resource_path))

    def hash_worker():
        while True:
            task = q.get()
            if task is None:
                break
            calculate_hashes(*task)
            q.task_done()

    q = Queue()
    threads = []
    for i in range(num_threads):
        t = Thread(target=hash_worker)
        threads.append(t)
        t.start()

    logger.info("%d resources to be checked" % (len(meta.get_resources())))
    for tpl in meta.get_resources():
        q.put(tpl)
    q.join()

    for thread in threads:
        q.put(None)

    for thread in threads:
        thread.join()
