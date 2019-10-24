from hashlib import md5, sha256
from binascii import hexlify
from ..util import make_logger

logger = make_logger(__name__)


# maximum of 10k chunks in an object, so the chunk size necessarily
# increases from the default of 8MB to 16MB to 32MB as the file size
# increases. almost all objects BPA have will fit in 8MB chunks
S3_CHUNK_SIZES = [(t * (1 << 20)) for t in (8, 16, 32, 64, 128)]
S3_HASH_FIELDS = ['s3etag_%d' % t for t in S3_CHUNK_SIZES]


def make_multipart(md5_s3part):
    if len(md5_s3part) == 0:
        return md5(b'').hexdigest()
    elif len(md5_s3part) == 1:
        return hexlify(md5_s3part[0]).decode('ascii')
    else:
        return '%s-%d' % (md5(b''.join(md5_s3part)).hexdigest(), len(md5_s3part))


def _generate_hashes(fd):
    md5_s3part = dict((t, []) for t in S3_CHUNK_SIZES)
    md5_whole = md5()
    sha256_whole = sha256()
    hashed = 0
    # note: the read length needs to be an integer multiple of
    # the block size of each hash (any large power of 2 is fine)
    while True:
        data = fd.read(S3_CHUNK_SIZES[-1])
        hashed += len(data)
        if len(data) == 0:
            break
        for chunk_size in S3_CHUNK_SIZES:
            for i in range(0, len(data), chunk_size):
                md5_s3part[chunk_size].append(md5(data[i:i + chunk_size]).digest())
        md5_whole.update(data)
        sha256_whole.update(data)
    obj = {
        'md5': md5_whole.hexdigest(),
        'sha256': sha256_whole.hexdigest(),
    }
    obj.update(dict(('s3etag_%d' % (cs), make_multipart(parts)) for (cs, parts) in md5_s3part.items()))
    return obj


def generate_hashes(fname):
    logger.info("generating hashes: %s" % (fname))
    with open(fname, 'rb') as fd:
        return _generate_hashes(fd)
