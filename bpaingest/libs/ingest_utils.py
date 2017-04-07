import unittest
import json
import re
from bpa_constants import BPA_PREFIX

from ..util import make_logger
import datetime

logger = make_logger(__name__)

bpa_id_re = re.compile(r'^102\.100\.100[/\.](\d+)$')
bpa_id_abbrev_re = re.compile(r'^(\d+)$')


def extract_bpa_id(s):
    "parse a BPA ID, with or without the prefix, returning with the prefix"
    if isinstance(s, float):
        s = int(s)
    if isinstance(s, int):
        s = str(s)
    # if someone has appended extraction number, remove it
    if '_' in s:
        s = s.rsplit('_', 1)[0]
    m = bpa_id_re.match(s)
    if m:
        return BPA_PREFIX + m.groups()[0]
    m = bpa_id_abbrev_re.match(s)
    if m:
        return BPA_PREFIX + m.groups()[0]
    logger.warning("unable to parse BPA ID: `%s'" % s)
    return None


def get_int(val, default=None):
    """
    get a int from a string containing other alpha characters
    """

    if isinstance(val, int):
        return val

    try:
        return int(get_clean_number(val, default))
    except TypeError:
        return default


number_find_re = re.compile(r'(-?\d+\.?\d*)')


def get_clean_number(val, default=None):
    if isinstance(val, float):
        return val

    if val is None:
        return default

    try:
        return float(val)
    except TypeError:
        pass
    except ValueError:
        pass

    matches = number_find_re.findall(unicode(val))
    if len(matches) == 0:
        return default
    return float(matches[0])


def strip_all(reader):
    """
    Scrub extra whitespace from values in the reader dicts as read from the csv files
    """

    from django.utils.encoding import smart_text
    entries = []
    for entry in reader:
        new_e = {}
        for k, v in entry.items():
            new_e[k] = smart_text(v.strip())
        entries.append(new_e)

    return entries


def get_date_isoformat(s):
    "try to parse the date, if we can, return the date as an ISO format string"
    dt = get_date(s)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d")


def get_date(dt):
    '''
    convert `dt` into a datetime.date, returning `dt` if it is already an
    instance of datetime.date. only two string date formats are supported:
    YYYY-mm-dd and dd/mm/YYYY. if conversion fails, returns None.
    '''

    if dt is None:
        return None

    if isinstance(dt, datetime.date):
        return dt

    if not isinstance(dt, basestring):
        return None

    if dt.strip() == '':
        return None

    try:
        return datetime.datetime.strptime(dt, '%Y-%m-%d').date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, '%Y-%b-%d').date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, '%d/%m/%Y').date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, '%d/%m/%y').date()
    except ValueError:
        pass

    logger.error('Date `{}` is not in a supported format'.format(dt))
    return None


def pretty_print_namedtuple(named_tuple):
    """
    pretty prints the namedtuple
    """

    def json_serial(obj):
        """
        JSON serializer for objects not serializable by default json code
        """

        if isinstance(obj, datetime.date):
            serial = obj.isoformat()
            return serial

    return json.dumps(named_tuple._asdict(), indent=4, default=json_serial)


class TestGetCleanNumber(unittest.TestCase):
    """
    get_clean_number tester
    """

    def setUp(self):
        self.floats = (12131.5345, 22.444, 33.0)
        self.strings = (('3.1415926535', 3.1415926535), ('-2.71828', -2.71828), ('37.1 degrees', 37.1))

    def test_get_clean_number(self):
        for f in self.floats:
            self.assertTrue(f == get_clean_number(f))

    def test_empty_string(self):
        self.assertIs(get_clean_number(''), None)

    def test_string(self):
        for s, f in self.strings:
            self.assertEqual(get_clean_number(s), f)

    def test_integer(self):
        self.assertEqual(get_clean_number(123), 123)

    def test_none(self):
        self.assertIs(get_clean_number(None), None)


if __name__ == '__main__':
    unittest.main()
