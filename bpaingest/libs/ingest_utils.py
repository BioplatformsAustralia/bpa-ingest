import datetime
import json
import math
import re
import inspect
import os

from .bpa_constants import BPA_PREFIX

ands_id_re = re.compile(r"^102\.100\.100[/\.](\d+)$")
ands_id_abbrev_re = re.compile(r"^(\d+)$")
# this format of BPA ID has been used in older projects (e.g. BASE)
ands_id_abbrev_2_re = re.compile(r"^102\.100\.\.100[/\.](\d+)$")
# <sample_id>_<extraction>
sample_extraction_id_re = re.compile(r"^\d{4,6}_\d")


def fix_pcr(logger, pcr):
    """ Check pcr value """
    val = pcr.strip()
    # header in the spreadsheet
    if val == "i.e. P or F":
        return None
    if val not in ("P", "F", ""):
        logger.error("PCR value is neither F, P or " ", setting to X: `%s'" % (val))
        val = "X"
    return val


def to_uppercase(logger, s):
    if s is None:
        return
    return str(s).upper()


def fix_sample_extraction_id(logger, val):
    if val is None:
        return val
    if isinstance(val, float) or isinstance(val, int):
        return "%s_1" % (int(val))
    val = str(val).strip().replace("-", "_")
    if val == "":
        return None
    # header row left in
    if val.startswith("e.g. "):
        return None
    if not sample_extraction_id_re.match(val):
        logger.warning("invalid sample_extraction_id: %s" % (val))
        return None
    return val


def make_sample_extraction_id(extraction_id, sample_id):
    # instructions from project manager: if no extraction_id in the spreadsheet,
    # append _1 to the sample_id_to_ckan_name
    return extraction_id or (sample_id.split("/")[-1] + "_1")


def fix_date_interval(logger, val):
    # 1:10 is in excel date format in some columns; convert back
    if isinstance(val, datetime.time):
        return "%s:%s" % (val.hour, val.minute)
    return val


def merge_pass_fail(row):
    # some of the early BASE/MM amplicon submission sheets have more than one pass fail column,
    # but only one should have real data (we key on 'dilution_used')
    dilution = row.dilution_used.strip().lower()
    if dilution == "neat":
        pass_fail_attrs = ("pass_fail", "pass_fail_neat")
    elif dilution == "1:10":
        pass_fail_attrs = ("pass_fail", "pass_fail_10")
    elif dilution == "1:100":
        pass_fail_attrs = ("pass_fail", "pass_fail_100")
    elif dilution == "2 x template" or dilution == "weak" or dilution == "2x":
        pass_fail_attrs = ("pass_fail",)
    else:
        raise Exception("unknown dilution: %s" % (dilution))
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


class ApiFqBuilder:
    def __init__(self, logger, key, value):
        self.fq = ""
        self._logger = logger
        self._key = key
        self._value = value

    def solr_date(self):
        self.build_value(build_solr_utc_for_date)
        return self

    def ands_is_for_whitelist(self):
        self.build_value(build_ands_is_for_whitelist)
        return self

    def build_value(self, func):
        self._value = func(self._logger, self._key, self._value)

    def build(self):
        # need to escape the colons
        self._value = escape_for_solr(self._value)
        return f"+{self._key}:{self._value}"

    @staticmethod
    def from_collection(logger, list_of_key_values):
        fq = ""
        for key, value in list_of_key_values.items():
            fq += (
                " "
                + ApiFqBuilder(logger, key, value)
                .ands_is_for_whitelist()
                .solr_date()
                .build()
            )
        return fq.strip()


def escape_for_solr(value):
    for c in [":"]:
        value = value.replace(c, "\\" + c)
    return value


def build_solr_utc_for_date(logger, key, value):
    if key in ["run_date"]:
        return date_to_solr(logger, value)
    return value


def date_to_solr(logger, datestring):
    dt = datetime.datetime.strptime(datestring, "%y%m%d")
    if dt is None:
        return datestring
    # datetimes aren't stored with timezones
    return f"{dt.isoformat()}Z"


def build_ands_is_for_whitelist(logger, key, value):
    if key in [
        "bpa_dataset_id",
        "dataset_id",
        "bpa_sample_id",
        "sample_id",
        "bpa_library_id",
        "library_id",
    ]:
        return extract_ands_id(logger, value)
    return value


def extract_ands_id(logger, s, silent=False):
    "parse a BPA ID, with or without the prefix, returning with the prefix"
    if isinstance(s, float):
        s = int(s)
    if isinstance(s, int):
        s = str(s)
    # if someone has appended extraction number, remove it
    s = s.strip()
    if s == "":
        return None
    # header row left in
    if s.startswith("e.g. "):
        return None
    # remove junk
    if s.startswith("don't use"):
        return None
    if s.startswith("missing"):
        return None
    if s.startswith("NA"):
        return None

    # duplicated 102.100.100: e.g. 102.100.100.102.100.100.25977
    s = s.replace("102.100.100.102.100.100.", "102.100.100/")
    # handle a sample extraction id tacked on the end with an underscore
    if "_" in s:
        s = s.rsplit("_", 1)[0]
    m = ands_id_re.match(s)
    if m:
        return BPA_PREFIX + m.groups()[0]
    m = ands_id_abbrev_re.match(s)
    if m:
        return BPA_PREFIX + m.groups()[0]
    m = ands_id_abbrev_2_re.match(s)
    if m:
        return BPA_PREFIX + m.groups()[0]
    if not silent:
        logger.warning("unable to parse BPA ID: `%s'" % s)
    return None


def extract_ands_id_silent(logger, s):
    return extract_ands_id(logger, s, silent=True)


def short_ands_id(logger, s):
    return extract_ands_id(logger, s).split("/")[-1]


def get_int(logger, val, default=None):
    """
    get a int from a string containing other alpha characters
    """

    if isinstance(val, int):
        return val

    try:
        return int(get_clean_number(logger, val, default))
    except TypeError:
        return default


def int_or_comment(logger, val):
    # fix up '14.0' type values coming through from Excel; if not an integer,
    # it's a note or a text code, which we just pass back unaltered
    if val is None:
        return None
    try:
        return str(int(float(val)))
    except ValueError:
        val = str(val).strip()
        if not val:
            return None
        return val


def date_or_int_or_comment(logger, val):
    if isinstance(val, datetime.date):
        return get_date_isoformat(logger, val)
    return int_or_comment(logger, val)


number_find_re = re.compile(r"(-?\d+\.?\d*)")


def get_clean_number(logger, val, default=None):
    if isinstance(val, float):
        return val

    if val is None:
        return default

    try:
        return float(val)
    except TypeError:
        logger.error("Invalid number - Type error: {} ".format(str(val)))
        return default
    except ValueError:
        if val != 'unknown':
            logger.error("Invalid number - Value error: {}".format(str(val)))
        return default

    matches = number_find_re.findall(str(val))
    if len(matches) == 0:
        return default
    return float(matches[0])


def get_date_isoformat(logger, s, silent=False):
    "try to parse the date, if we can, return the date as an ISO format string"
    dt = _get_date(logger, s, silent)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d")


def get_date_isoformat_as_datetime(logger, s, silent=False):
    "try to parse the date, if we can, return the date as an ISO format string"
    dt = _get_date(logger, s, silent)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%d_%H:%M:%S")


def get_time(logger, s):
    return str(s)


def _get_date(logger, dt, silent=False):
    """
    Convert `dt` into a datetime.date, returning `dt` if it is already an
    instance of datetime.date. 
    
    The following date formats are supported:
       YYYY-mm-dd
       dd/mm/YYYY 
       dd-mm-YYYY
       dd.mm.YYYY
       dd.mm.YY

       YYYY-mm (convert to first date of month)
       mm/YYYY (convert to first date of month)
    
    If conversion fails, returns None.
    """

    if dt is None:
        return None

    if (
        dt == "unknown"
        or dt == "Unknown"
        or dt == "UnkNown"
        or dt == "unkNown"
        or dt == "event date not recorded"
        or dt == "Not yet assigned"
        or dt == "Not applicable"
        or dt == "not applicable"
        or dt == "no information"
        or dt == "Not submitted"
        or dt == "not determined"
        or dt == "To be filled in"
        or dt == "(null)"
        or dt == "NA"
        or dt == "n/a"
        or dt == "TBA"
    ):
        return None

    if isinstance(dt, datetime.date):
        return dt

    if not isinstance(dt, str):
        return None

    if dt.strip() == "":
        return None

    try:
        return datetime.datetime.strptime(dt, "%Y-%m-%d").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%Y-%m").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%Y-%b-%d").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%d/%m/%Y").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%d-%m-%Y").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%d.%m.%Y").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%d.%m.%y").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%m/%Y").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%d/%m/%y").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%y-%m-%d %H:%M:%S").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%SZ").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%MZ").date()
    except ValueError:
        pass

    if not silent:
        logger.error("Date `{}` is not in a supported format".format(dt))
    return None


def add_spatial_extra(logger, package):
    "add a ckanext-spatial extra to the package which has a longitude and latitude"
    lat = get_clean_number(logger, package.get("latitude"))
    lng = get_clean_number(logger, package.get("longitude"))
    if not lat or not lng:
        # Ensure spatial field is cleared
        package["spatial"] = ""
        return
    geo = {"type": "Point", "coordinates": [lng, lat]}
    package["spatial"] = json.dumps(geo, sort_keys=True)


def permissions_organization_member(logger, obj):
    obj["private"] = False
    obj["resource_permissions"] = "organization_member"


def permissions_organization_member_after_embargo(
    logger, obj, field_name, days, consortium_org
):
    obj["private"] = False
    if field_name not in obj:
        logger.error(
            "permission field {} not found in obj (keys are {})".format(
                field_name, sorted(obj.keys())
            )
        )
    obj["resource_permissions"] = "organization_member_after_embargo:{}:{}:{}".format(
        field_name, days, consortium_org
    )


def permissions_public(logger, obj):
    obj["private"] = False
    obj["resource_permissions"] = "public"


def get_year(logger, s):
    if re.search(r"\d{4}\.\d*", s):
        # remove decimal and convert back to string
        return str(math.trunc(float(s)))
    else:
        return get_date_isoformat(logger, s)


def date_or_str(logger, v):
    d = get_date_isoformat(logger, v, silent=True)
    if d is not None:
        return d
    as_string = str(v)
    # only round to integer if decimal places are 0
    as_string_groups = re.search(r"(\d{4})(\.0|)", as_string)
    return as_string_groups.group(1) if as_string_groups else as_string


def from_comma_or_space_separated_to_list(logger, raw):
    separators = [",", " ", "\n"]
    if re.search(" ", raw) and re.search(",", raw) and re.search("\n", raw):
        raise Exception(
            "There are spaces and commas and newlines in this string. Only commas OR spaces OR newlines can be used as data separators."
        )
    for next_separator in separators:
        result = raw.split(next_separator)
        if len(result) > 1:
            return result
    raise Exception("Raw input must be separated by one of {}".format(separators))


def apply_access_control(logger, metadata, obj):
    # access_control_mode is set here, but not by the user
    # usually set closed when object is created, but may have been
    # changed elsewhere

    # (EMPTY,EMPTY,NO DEFAULT) = open
    # (EMPTY,EMPTY,DEFAULT) = date
    # (DATE, anything, anything) = date
    # (EMPTY, REASON, anything) = closed

    def _log_access_control_error(logger, obj):
        # 0 represents this line
        # 1 represents line at caller
        callerframerecord = inspect.stack()[1]
        frame = callerframerecord[0]
        info = inspect.getframeinfo(frame)
        logger.error(
            "Missing access control field - {}:{}".format((info.filename.split(os.path.sep))[-1], info.lineno)
        )

        obj["access_control_mode"] = "closed"
        obj["access_control_date"] = ""
        obj[
            "access_control_reason"
        ] = "Unable to determine correct embargo period. {}".format(
            obj.get("access_control_reason", "")
        ).rstrip()

    # date of transfer is needed for calculations
    if (
	"date_of_transfer" not in obj
    ):
        _log_access_control_error(logger, obj)
        return

    if (
        "access_control_mode" not in obj
        or "access_control_date" not in obj
        or "access_control_reason" not in obj
    ):
        obj.setdefault("access_control_mode","closed")
        obj.setdefault("access_control_date","")
        obj.setdefault("access_control_reason","")

    if obj["access_control_mode"] in (None, ""):
        _log_access_control_error(logger, obj)
        return

    obj["access_control_mode"] = obj["access_control_mode"].lower()

    # mode has been set to open elsewhere
    if obj["access_control_mode"] in ("open"):
        return

    # Need transfer date for rest

    transfer_date = _get_date(logger, obj.get("date_of_transfer", None))
    if transfer_date is None:
        # can't parse the date of transfer
        _log_access_control_error(logger, obj)
        return

    # if date field is empty
    access_date = obj.get("access_control_date","")
    if not access_date or not access_date.strip():
        # if default defined and no reason given, use default
        if getattr(metadata, "embargo_days", None) and not obj["access_control_reason"].strip():
            # date becomes date of transfer plus default embargo period
            embargo_days = int(getattr(metadata, "embargo_days"))
            obj["access_control_mode"] = "date"
            obj["access_control_date"] = get_date_isoformat(
                logger, transfer_date + datetime.timedelta(days=embargo_days)
            )
            return
        # but we have a reason, but no date - assume closed
        elif obj["access_control_reason"].strip():
            obj["access_control_mode"] = "closed"
            return
        # no date, no reason - assume open
        else:
            obj["access_control_mode"] = "open"
            return

    # date in normal formats
    embargo_date = _get_date(logger, obj.get("access_control_date", None), silent=True)
    if embargo_date:
        #  check if earlier than date of transfer, if so note error
        if embargo_date <= transfer_date:
            _log_access_control_error(logger, obj)
            logger.error("Embargo date is before Transfer date")
            obj["access_control_date"] = ""
            return
        obj["access_control_mode"] = "date"
        obj["access_control_date"] = get_date_isoformat(logger, embargo_date)
        return

    # if date is integer, greater than zero and less than five years (1827 days max)
    #     date becomes date of transfer integer
    embargo_days = None
    try:
        embargo_days = int(obj["access_control_date"])
        if embargo_days > 1827:
            # looks too much like a year, only support up to five years
            _log_access_control_error(logger, obj)
            logger.error("Integer Embargo is out of range")
            obj["access_control_date"] = ""
            return

        obj["access_control_mode"] = "date"
        obj["access_control_date"] = get_date_isoformat(
            logger, transfer_date + datetime.timedelta(days=embargo_days)
        )
        return
    except ValueError:
        pass

    # if date fails to parses
    if obj["access_control_mode"] not in ("date",):
        _log_access_control_error(logger, obj)
        return

    # We shouldn't get here
    _log_access_control_error(logger, obj)

def get_clean_doi(logger, val):
    if not val:
        return val

    try:
        val.index("doi")
    except ValueError:
        logger.error("DOI not found in: {}".format(val))
        return None

    # change any weblinks back to doi:
    regex = r"^https?:\/\/(dx\.)?doi.org\/"
    subst = "doi:"

    val = re.sub(regex, subst, val, 0)

    # check if DOI looks valid
    # regex is not exhaustive
    # See: https://www.crossref.org/blog/dois-and-matching-regular-expressions/

    if not re.match(r"^doi:10.\d{4,9}\/[-._;()\/:A-Z0-9]+$", val, re.IGNORECASE):
        logger.error("DOI does not look valid: {}".format(val))
        return None

    return val
