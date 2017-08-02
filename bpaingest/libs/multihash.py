from hashlib import md5, sha256
from binascii import hexlify
from ..util import make_logger

logger = make_logger(__name__)


S3_CHUNK_SIZE = 8 * (1 << 20)
S3_HASH_FIELD = 's3etag_%d' % (S3_CHUNK_SIZE)


def _generate_hashes(fd):
    md5_s3part = []
    md5_whole = md5()
    sha256_whole = sha256()
    hashed = 0
    # note: S3_CHUNK_SIZE needs to be an integer multiple of
    # the block size of each hash (any large power of 2 is fine)
    while True:
        data = fd.read(S3_CHUNK_SIZE)
        hashed += len(data)
        if len(data) == 0:
            break
        md5_s3part.append(md5(data).digest())
        md5_whole.update(data)
        sha256_whole.update(data)
    if len(md5_s3part) == 0:
        s3_etag = md5(b'').hexdigest()
    elif len(md5_s3part) == 1:
        s3_etag = hexlify(md5_s3part[0]).decode('ascii')
    else:
        s3_etag = '%s-%d' % (md5(b''.join(md5_s3part)).hexdigest(), len(md5_s3part))
    return {
        'md5': md5_whole.hexdigest(),
        S3_HASH_FIELD: s3_etag,
        'sha256': sha256_whole.hexdigest(),
    }


def generate_hashes(fname):
    logger.info("generating hashes: %s" % (fname))
    with open(fname, 'rb') as fd:
        return _generate_hashes(fd)
