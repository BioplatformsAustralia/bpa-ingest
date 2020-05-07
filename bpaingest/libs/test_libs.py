from io import BytesIO

from .ingest_utils import get_clean_number
from .multihash import _generate_hashes
from bpaingest.libs.common_resources import bsd_md5_re, linux_md5_re
from bpaingest.util import make_logger


logger = make_logger(__name__)

TEST_CHUNK_SIZE = 8 * (1 << 20)


def test_get_clean_number():
    floats = (12131.5345, 22.444, 33.0)
    strings = (
        ("3.1415926535", 3.1415926535),
        ("-2.71828", -2.71828),
        ("37.1 degrees", 37.1),
    )
    for f in floats:
        assert f == get_clean_number(logger, f)
    for s, f in strings:
        assert get_clean_number(logger, s) == f
    assert get_clean_number(logger, "") is None
    assert get_clean_number(logger, 123) == 123
    assert get_clean_number(logger, None) is None


def test_multihash_empty():
    result = _generate_hashes(BytesIO(b""))
    assert result == {
        "md5": "d41d8cd98f00b204e9800998ecf8427e",
        "s3etag_16777216": "d41d8cd98f00b204e9800998ecf8427e",
        "s3etag_33554432": "d41d8cd98f00b204e9800998ecf8427e",
        "s3etag_8388608": "d41d8cd98f00b204e9800998ecf8427e",
        "s3etag_134217728": "d41d8cd98f00b204e9800998ecf8427e",
        "s3etag_67108864": "d41d8cd98f00b204e9800998ecf8427e",
        "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    }


def test_multihash_one_chunk():
    result = _generate_hashes(BytesIO(b"hello" * (TEST_CHUNK_SIZE // 8)))
    assert result == {
        "md5": "22b0d6ae2d06788edab665b4bc2c1139",
        "s3etag_16777216": "22b0d6ae2d06788edab665b4bc2c1139",
        "s3etag_33554432": "22b0d6ae2d06788edab665b4bc2c1139",
        "s3etag_8388608": "22b0d6ae2d06788edab665b4bc2c1139",
        "s3etag_134217728": "22b0d6ae2d06788edab665b4bc2c1139",
        "s3etag_67108864": "22b0d6ae2d06788edab665b4bc2c1139",
        "sha256": "50c5711f72196fb29755b590d503d7772c8e7953b37f30ff5d7baf7445091490",
    }


def test_multihash_several_chunks():
    result = _generate_hashes(BytesIO(b"hello" * TEST_CHUNK_SIZE))
    assert result == {
        "md5": "84acd225412524e1706e97655fb068d6",
        "s3etag_16777216": "7a66de6f3254139221523a99e05edb5b-3",
        "s3etag_33554432": "40b449c188f1ac64474a99e07b3f1e65-2",
        "s3etag_8388608": "75fb13365cb1a544f03c6558e0fe1497-5",
        "s3etag_134217728": "84acd225412524e1706e97655fb068d6",
        "s3etag_67108864": "84acd225412524e1706e97655fb068d6",
        "sha256": "38acde04302e7136c04eddde7b04b03084abd415a5d1e2433b17243153ef8d4a",
    }


def test_multihash_huge_chunks():
    result = _generate_hashes(BytesIO(b"hello" * TEST_CHUNK_SIZE))
    assert result == {
        "md5": "84acd225412524e1706e97655fb068d6",
        "s3etag_16777216": "7a66de6f3254139221523a99e05edb5b-3",
        "s3etag_33554432": "40b449c188f1ac64474a99e07b3f1e65-2",
        "s3etag_8388608": "75fb13365cb1a544f03c6558e0fe1497-5",
        "s3etag_134217728": "84acd225412524e1706e97655fb068d6",
        "s3etag_67108864": "84acd225412524e1706e97655fb068d6",
        "sha256": "38acde04302e7136c04eddde7b04b03084abd415a5d1e2433b17243153ef8d4a",
    }


def test_md5lines():
    filenames = [
        "MD5 (24721-24724_and_24726-24729_SC_MA_Bio21-GCMS-001_857_PCA_median_normalised.png) = 8f819a7635f192212300cd64d1e34f10",
        "MD5 (24721-24724_and_24726-24729_SC_MA_Bio21-GCMS-001_857_PCA_median_normalised.png) =8f819a7635f192212300cd64d1e34f10",
        "MD5 (24721-24724_and_24726-24729_SC_MA_Bio21-GCMS-001_857_PCA_median_normalised.png)= 8f819a7635f192212300cd64d1e34f10",
        "MD5(24721-24724_and_24726-24729_SC_MA_Bio21-GCMS-001_857_PCA_median_normalised.png) = 8f819a7635f192212300cd64d1e34f10",
        "MD5(24721-24724_and_24726-24729_SC_MA_Bio21-GCMS-001_857_PCA_median_normalised.png)= 8f819a7635f192212300cd64d1e34f10",
        "MD5(24721-24724_and_24726-24729_SC_MA_Bio21-GCMS-001_857_PCA_median_normalised.png) =8f819a7635f192212300cd64d1e34f10",
        "MD5(33163-33189_SC_MA_LCMS_Bio21_LCQTOF-Agilent_623_20190620.xlsx)= 0a9d6f39f6f36b3984e9e31d8ff5819e",
    ]
    for filename in filenames:
        assert bsd_md5_re.match(filename) is not None


def test_md5lines2():
    filenames = [
        "8f819a7635f192212300cd64d1e34f10 24726-24729_SC_MA_Bio21-GCMS-001_857_PCA_median_normalised.png",
        "0a9d6f39f6f36b3984e9e31d8ff5819e 33163-33189_SC_MA_LCMS_Bio21_LCQTOF-Agilent_623_20190620.xlsx",
    ]
    for filename in filenames:
        assert linux_md5_re.match(filename) is not None
