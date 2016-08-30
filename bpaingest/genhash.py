import progressbar
import urlparse
import ckanapi
import os

from hashlib import md5, sha256
from binascii import hexlify
from .ops import ckan_method
from .util import make_logger

logger = make_logger(__name__)

S3_CHUNK_SIZE = 8 * (1 << 20)


def generate_hashes(fname):
    md5_s3part = []
    md5_whole = md5()
    sha256_whole = sha256()
    logger.info("generating hashes: %s" % (fname))
    total_size = os.stat(fname).st_size
    hashed = 0
    bar = progressbar.ProgressBar(max_value=total_size)
    # note: S3_CHUNK_SIZE needs to be an integer multiple of
    # the block size of each hash (any large power of 2 is fine)
    with open(fname, 'rb') as fd:
        while True:
            data = fd.read(S3_CHUNK_SIZE)
            hashed += len(data)
            bar.update(hashed)
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
        's3etag_%d' % (S3_CHUNK_SIZE): s3_etag,
        'sha256': sha256_whole.hexdigest(),
    }


def localpath(mirror_path, legacy_url):
    return os.path.join(
        mirror_path,
        urlparse.urlparse(legacy_url).path.lstrip('/'))


def genhash(ckan, meta, mirror_path):
    for bpa_id, legacy_url, resource in meta.get_resources():
        try:
            ckan_resource = ckan_method(ckan, 'resource', 'show')(id=resource['id'])
        except ckanapi.errors.NotFound:
            logger.error("resource `%s': not in CKAN, skipping" % (ckan_resource['id']))
            continue
        if len(ckan_resource.get('sha256', '')) == 64:
            logger.info("resource `%s': already hashed, continuing" % (ckan_resource['id']))
            continue
        fpath = localpath(mirror_path, legacy_url)
        hashes = generate_hashes(fpath)
        if hashes['md5'] != resource['md5']:
            logger.error("md5 mismatch, have `%s' and expected `%s': %s" % (hashes['md5'], resource['md5'], fpath))
            continue
        patch_obj = hashes.copy()
        patch_obj['id'] = ckan_resource['id']
        ckan_method(ckan, 'resource', 'patch')(**patch_obj)
        logger.info("resource `%s': hashes calculated and pushed" % (ckan_resource['id']))
