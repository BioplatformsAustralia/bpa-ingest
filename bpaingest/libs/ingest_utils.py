import string
import unittest
import json

from ..util import make_logger
import datetime

logger = make_logger(__name__)

# list of chars to delete
remove_letters_map = dict((ord(char), None) for char in string.punctuation + string.ascii_letters)


def get_clean_number(val, default=None, debug=False):
    """
    Try to clean up numbers
    """

    if debug:
        logger.debug(val)

    if val in (None, ""):
        return default

    if isinstance(val, int):
        return val

    if isinstance(val, float):
        return val

    # remove_letters_map = dict((ord(char), None) for char in string.letters)
    try:
        return int(val.translate(remove_letters_map))
    except ValueError:
        return default


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


def get_clean_float(val, default=None, stringconvert=True):
    """
    Try to hammer an arb value into a float.
    If stringconvert is true (the default behaviour), try to convert the string to a float,
    if not, return the given default value
    """

    def to_float(var):
        try:
            return float(var)
        except ValueError:
            logger.warning("ValueError Value '{0}' not floatable, returning default '{1}'".format(var, default))
            return default
        except TypeError:
            logger.warning("TypeError Value '{0}' not floatable, returning default '{1}'".format(var, default))
            return default

    # if its a float, its probably ok
    if isinstance(val, float):
        return val

    # if its an integer, make it a float
    if isinstance(val, int):
        return to_float(val)

    # the empty string gets the default
    if val == '':
        return default

    # if its not a string, forget it, return the default
    if not isinstance(val, basestring):
        return default

    if stringconvert:
        return to_float(filter(lambda x: x.isdigit(), val))
    else:
        return default


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
        return datetime.datetime.strptime(dt, '%d/%m/%Y').date()
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


class TestGetCleanFloat(unittest.TestCase):
    """
    get_clean_float tester
    """

    def setUp(self):
        self.floats = (12131.5345, 22.444, 33.0)

    def test_get_clean_float(self):
        for f in self.floats:
            self.assertTrue(f == get_clean_float(f))

    def test_xxx(self):
        self.assertTrue(get_clean_float('', 'XXX') == 'XXX')

    def test_none(self):
        self.assertTrue(get_clean_float('') is None)

    def test_int(self):
        self.assertTrue(get_clean_float(123) == 123)


if __name__ == '__main__':
    unittest.main()
