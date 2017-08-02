from .ingest_utils import get_clean_number


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
