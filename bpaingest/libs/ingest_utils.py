import json
import re
from .bpa_constants import BPA_PREFIX

import datetime

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
    # duplicated 102.100.100: e.g. 102.100.100.102.100.100.25977
    s.replace("102.100.100.102.100.100.", "102.100.100/")
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
        return int(get_clean_number(val, default))
    except TypeError:
        return default


number_find_re = re.compile(r"(-?\d+\.?\d*)")


def get_clean_number(logger, val, default=None):
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


def get_time(logger, s):
    return str(s)


def _get_date(logger, dt, silent=False):
    """
    convert `dt` into a datetime.date, returning `dt` if it is already an
    instance of datetime.date. only two string date formats are supported:
    YYYY-mm-dd and dd/mm/YYYY. if conversion fails, returns None.
    """

    if dt is None:
        return None

    if (
        dt == "unknown"
        or dt == "Unknown"
        or dt == "Not yet assigned"
        or dt == "Not applicable"
        or dt == "(null)"
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
        return datetime.datetime.strptime(dt, "%Y-%b-%d").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%d/%m/%Y").date()
    except ValueError:
        pass

    try:
        return datetime.datetime.strptime(dt, "%d/%m/%y").date()
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
        return
    geo = {"type": "Point", "coordinates": [lng, lat]}
    package["spatial"] = json.dumps(geo, sort_keys=True)


def permissions_organization_member(logger, obj):
    obj["private"] = True  # NB: placeholder, should be public when we go live
    obj["resource_permissions"] = "organization_member"


def permissions_organization_member_after_embargo(
    logger, obj, field_name, days, consortium_org
):
    obj["private"] = True  # NB: placeholder, should be public when we go live
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


def date_or_str(logger, v):
    d = get_date_isoformat(logger, v, silent=True)
    if d is not None:
        return d
    return v
