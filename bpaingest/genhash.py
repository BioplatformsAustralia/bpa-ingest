import urllib.parse
import ckanapi
import os

from threading import Thread
from queue import Queue
from hashlib import md5, sha256
from binascii import hexlify
from .ops import ckan_method
from .util import make_logger

logger = make_logger(__name__)

S3_CHUNK_SIZE = 8 * (1 << 20)
S3_HASH_FIELD = 's3etag_%d' % (S3_CHUNK_SIZE)


def generate_hashes(fname):
    md5_s3part = []
    md5_whole = md5()
    sha256_whole = sha256()
    logger.info("generating hashes: %s" % (fname))
    hashed = 0
    # note: S3_CHUNK_SIZE needs to be an integer multiple of
    # the block size of each hash (any large power of 2 is fine)
    with open(fname, 'rb') as fd:
        while True:
            data = fd.read(S3_CHUNK_SIZE)
            hashed += len(data)
            if len(data) == 0:
                break
            md5_s3part.append(md5(data).digest())
            md5_whole.update(data)
            sha256_whole.update(data)
    if len(md5_s3part) == 1:
        s3_etag = hexlify(md5_s3part[0])
    else:
        s3_etag = '%s-%d' % (md5(''.join(md5_s3part)).hexdigest(), len(md5_s3part))
    return {
        'md5': md5_whole.hexdigest(),
        S3_HASH_FIELD: s3_etag,
        'sha256': sha256_whole.hexdigest(),
    }


def localpath(mirror_path, legacy_url):
    path = urllib.parse.urlparse(legacy_url).path
    if path.startswith('/bpa/'):
        path = path[5:]
    path = path.lstrip('/')
    return os.path.join(mirror_path, path)


def genhash(ckan, meta, mirror_path, num_threads):
    def calculate_hashes(bpa_id, legacy_url, resource):
            try:
                ckan_resource = ckan_method(ckan, 'resource', 'show')(id=resource['id'])
            except ckanapi.errors.NotFound:
                logger.error("resource `%s': not in CKAN, skipping" % (resource['id']))
                return
            if len(ckan_resource.get('sha256', '')) == 64:
                logger.info("resource `%s': already hashed, continuing" % (ckan_resource['id']))
                return
            fpath = localpath(mirror_path, legacy_url)
            hashes = generate_hashes(fpath)
            if hashes['md5'] != resource['md5']:
                logger.critical("MD5 hash mismatch of on-disk data. Have `%s' and expected `%s': %s" % (hashes['md5'], resource['md5'], fpath))
                return
            patch_obj = hashes.copy()
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
