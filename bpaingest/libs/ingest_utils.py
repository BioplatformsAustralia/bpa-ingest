import unittest
import json
import re
from bpa_constants import BPA_PREFIX

from ..util import make_logger
import datetime

logger = make_logger(__name__)

bpa_id_re = re.compile(r'^102\.100\.100[/\.](\d+)$')
bpa_id_abbrev_re = re.compile(r'^(\d+)$')
# this format of BPA ID has been used in older projects (e.g. BASE)
bpa_id_abbrev_2_re = re.compile(r'^102\.100\.\.100[/\.](\d+)$')
# <BPA_ID>_<extraction>
sample_extraction_id_re = re.compile(r'^\d{4,6}_\d')


def fix_pcr(pcr):
    """ Check pcr value """

    val = pcr.encode('utf-8').strip()
    # header in the spreadsheet
    if val == 'i.e. P or F':
        return None
    if val not in ("P", "F", ""):
        logger.error("PCR value is neither F, P or " ", setting to X: `%s'" % (val))
        val = "X"
    return val


def fix_sample_extraction_id(val):
    if val is None:
        return val
    if type(val) is float or type(val) is int:
        return '%s_1' % (int(val))
    val = unicode(val).strip().replace('-', '_')
    if val == '':
        return None
    if not sample_extraction_id_re.match(val):
        logger.warning("invalid sample_extraction_id: %s" % (val))
        return None
    return val


def make_sample_extraction_id(extraction_id, bpa_id):
    # instructions from project manager: if no extraction_id in the spreadsheet,
    # append _1 to the bpa_id_to_ckan_name
    return extraction_id or (bpa_id.split('.')[-1] + "_1")


def fix_date_interval(val):
    # 1:10 is in excel date format in some columns; convert back
    if isinstance(val, datetime.time):
        return '%s:%s' % (val.hour, val.minute)
    return val


def merge_pass_fail(row):
    # some of the early BASE/MM amplicon submission sheets have more than one pass fail column,
    # but only one should have real data (we key on 'dilution_used')
    dilution = row.dilution_used.strip().lower()
    if dilution == 'neat':
        pass_fail_attrs = ('pass_fail', 'pass_fail_neat')
    elif dilution == '1:10':
        pass_fail_attrs = ('pass_fail', 'pass_fail_10')
    elif dilution == '1:100':
        pass_fail_attrs = ('pass_fail', 'pass_fail_100')
    else:
        raise Exception('unknown dilution: %s' % (dilution))
    vals = []
    for attr in pass_fail_attrs:
        v = getattr(row, attr)
        if v:
            vals.append(v)
    if len(vals) == 0:
        return None
    elif len(vals) == 1:
        return vals[0]
    raise Exception("more than one amplicon pass_fail column value: %s" % (vals))


def extract_bpa_id(s):
    "parse a BPA ID, with or without the prefix, returning with the prefix"
    if isinstance(s, float):
        s = int(s)
    if isinstance(s, int):
        s = str(s)
    # if someone has appended extraction number, remove it
    s = s.strip()
    if s == '':
        return None
    # handle a sample extraction id tacked on the end with an underscore
    if '_' in s:
        s = s.rsplit('_', 1)[0]
    m = bpa_id_re.match(s)
    if m:
        return BPA_PREFIX + m.groups()[0]
    m = bpa_id_abbrev_re.match(s)
    if m:
        return BPA_PREFIX + m.groups()[0]
    m = bpa_id_abbrev_2_re.match(s)
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
