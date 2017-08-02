from io import BytesIO

from .ingest_utils import get_clean_number
from .multihash import _generate_hashes, S3_CHUNK_SIZE


def test_get_clean_number():
    floats = (12131.5345, 22.444, 33.0)
    strings = (('3.1415926535', 3.1415926535), ('-2.71828', -2.71828), ('37.1 degrees', 37.1))
    for f in floats:
        assert(f == get_clean_number(f))
    for s, f in strings:
        assert(get_clean_number(s) == f)
    assert(get_clean_number('') is None)
    assert(get_clean_number(123) == 123)
    assert(get_clean_number(None) is None)


def test_multihash_empty():
    result = _generate_hashes(BytesIO(b''))
    assert(result == {
        'md5': 'd41d8cd98f00b204e9800998ecf8427e',
        's3etag_8388608': 'd41d8cd98f00b204e9800998ecf8427e',
        'sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    })


def test_multihash_one_chunk():
    result = _generate_hashes(BytesIO(b'hello' * (S3_CHUNK_SIZE // 8)))
    assert(result == {
        'md5': '22b0d6ae2d06788edab665b4bc2c1139',
        'sha256': '50c5711f72196fb29755b590d503d7772c8e7953b37f30ff5d7baf7445091490',
        's3etag_8388608': '22b0d6ae2d06788edab665b4bc2c1139'
    })


def test_multihash_several_chunks():
    result = _generate_hashes(BytesIO(b'hello' * S3_CHUNK_SIZE))
    assert(result == {
        's3etag_8388608': '75fb13365cb1a544f03c6558e0fe1497-5',
        'sha256': '38acde04302e7136c04eddde7b04b03084abd415a5d1e2433b17243153ef8d4a',
        'md5': '84acd225412524e1706e97655fb068d6'
    })
