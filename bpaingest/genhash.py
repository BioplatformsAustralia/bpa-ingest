import urllib.parse
import ckanapi
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


def genhash(ckan, meta, mirror_path, num_threads):
    def calculate_hashes(bpa_id, legacy_url, resource):
        fpath = localpath(mirror_path, legacy_url)
        patch_obj = {}

        try:
            ckan_resource = ckan_method(ckan, 'resource', 'show')(id=resource['id'])
        except ckanapi.errors.NotFound:
            logger.error("resource `%s': not in CKAN, skipping" % (resource['id']))
            return

        size = ckan_resource.get('size', '')
        if size != '':
            patch_obj['size'] = str(os.stat(fpath).st_size)

        if len(ckan_resource.get('sha256', '')) != 64:
            hashes = generate_hashes(fpath)
            if hashes['md5'] != resource['md5']:
                logger.critical("MD5 hash mismatch of on-disk data. Have `%s' and expected `%s': %s" % (hashes['md5'], resource['md5'], fpath))
                return
            patch_obj.update(hashes)

        if not patch_obj:
            logger.info("resource `%s': already hashed, continuing" % (ckan_resource['id']))
            return

        patch_obj['id'] = ckan_resource['id']
        ckan_method(ckan, 'resource', 'patch')(**patch_obj)
        logger.info("resource `%s': hashes calculated and pushed" % (ckan_resource['id']))

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
