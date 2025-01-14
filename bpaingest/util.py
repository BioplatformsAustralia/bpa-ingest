import csv
import datetime
import logging
import os
import re
import string
from collections import namedtuple
from hashlib import md5
from .libs.munge import bpa_munge_filename

import ckanapi
from dateutil.relativedelta import relativedelta


def one(l):
    if len(l) != 1:
        raise Exception("Expected one element, got {}: {}".format(len(l), l))
    return l[0]


def sample_id_to_ckan_name(sample_id, suborg=None, postfix=None):
    r = "bpa-"
    if suborg is not None:
        r += suborg + "-"
    r += sample_id.replace("/", "_").replace(".", "_").replace(" ", "")
    if postfix is not None:
        r += "-" + postfix
    # CKAN insists upon lowercase
    return r.lower()


def validate_write_reuploads_interval(logger, args):
    if not args.write_reuploads_interval:
        return None
    if args.write_reuploads_interval and not args.write_reuploads:
        raise Exception(
            f"To use cache reuploads write interval, the interval must be an integer and cache write reuploads must be enabled."
        )
    logger.info(
        f"Activated write reuploads interval of {args.write_reuploads_interval}"
    )
    return args.write_reuploads_interval


def make_reuploads_cache_path(logger, args):
    if not args.read_reuploads and not args.write_reuploads:
        return None
    if not args.download_path or not args.project_name:
        raise Exception(
            "To use cache reuploads, download_path arg (and project) must also be set."
        )
    reuploads_dir = os.path.join(args.download_path, args.project_name)
    os.makedirs(reuploads_dir, exist_ok=True)
    reupload_path = os.path.join(reuploads_dir, "reupload_resources.dump")
    msg_activation = f"Activated reupload cache at {reupload_path} for"
    if args.read_reuploads:
        msg_activation += " reads"
    if args.write_reuploads:
        msg_activation += " writes"
    logger.info(msg_activation)
    return reupload_path


def prune_dict(d, keys):
    if d is None:
        return None
    return dict((k, v) for (k, v) in list(d.items()) if k in keys)


def clean_tag_name(s):
    "reduce s to strings acceptable in a tag name"
    s = s.replace("+", "_")
    s = s.rstrip()
    return "".join(
        t for t in s if t in string.digits or t in string.ascii_letters or t in "-_. "
    )


def make_registration_decorator():
    """
    returns a (decorator, list). any function decorated with
    the returned decorator will be appended to the list
    """
    registered = []

    def _register(fn):
        registered.append(fn)
        return fn

    return _register, registered


def make_logger(name, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.propagate = False
    logger.setLevel(level)
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s [%(levelname)-7s] [%(name)s]  %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger


# Decorator to put around functions whilst debugging
def logger_wrap(func):
    def wrap(*args, **kwargs):
        # Log the function name and arguments
        logger.warn(f"Calling {func.__name__} with args: {args}, kwargs: {kwargs}")

        # Call the original function
        result = func(*args, **kwargs)

        # Log the return value
        logger.warn(f"{func.__name__} returned: {result}")

        # Return the result
        return result

    return wrap


def make_ckan_api(args):
    ckan = ckanapi.RemoteCKAN(
        args.ckan_url, apikey=args.api_key, verify_ssl=args.verify_ssl
    )
    return ckan


CKAN_AUTH = {"login": "CKAN_USERNAME", "password": "CKAN_PASSWORD"}


digit_words = {
    "0": "zero",
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
}


def csv_to_named_tuple(
    typname,
    fname,
    mode="r",
    additional_context=None,
    cleanup=None,
    name_fn=None,
    dialect="excel",
    skip=0,
):
    if fname is None:
        return [], []

    def clean_name(s):
        s = s.lower().strip().replace("-", "_").replace(" ", "_")
        s = "".join(
            [
                t
                for t in s
                if t in string.ascii_letters or t in string.digits or t == "_"
            ]
        )
        if len(s) > 0:
            if s[0] in string.digits:
                s = digit_words[s[0]] + s[1:]
            s = s.strip("_")
            s = re.sub(r"__+", "_", s).strip("_")
            # reserved words aren't permitted
            if s == "class":
                s = "class_"
        else:  # we have a blank column heading
            s = "noHeading"
        return s

    def default_name_fn(s):
        return s

    additional_keys = []
    if additional_context is not None:
        additional_keys += list(sorted(additional_context.keys()))
    if name_fn is None:
        name_fn = default_name_fn
    with open(fname, mode) as fd:
        r = csv.reader(fd, dialect=dialect)

        # skip non-header rows before header
        for nhr in range(skip):
            next(r)

        # read header
        header = [name_fn(clean_name(t)) for t in next(r)] + additional_keys
        typ = namedtuple(typname, header)

        # read data
        rows = []
        for row in r:
            if cleanup is not None:
                row = [cleanup(t) for t in row]
            rows.append(typ(*(row + [additional_context[t] for t in additional_keys])))
        return header, rows


def strip_to_ascii(s):
    return "".join([t for t in s if ord(t) < 128])


def common_values(dicts):
    """
    given a list of dicts, return a dict with only the values shared
    in common between those dicts
    """
    # bullet-proof this against being handed an iterator
    dicts = list(dicts)
    all_keys = set()
    for d in dicts:
        all_keys = all_keys.union(set(d.keys()))
    r = {}
    for k in all_keys:
        vals = set([d.get(k) for d in dicts])
        if len(vals) == 1:
            r[k] = dicts[0][k]
    return r


def merge_values(key, sep, dicts):
    """
    given a list of dicts, return a dict with the set of values
    for a specific key joined by the seperator provided
    """
    # bullet-proof this against being handed an iterator
    dicts = list(dicts)
    all_keys = set()
    for d in dicts:
        all_keys = all_keys.union(set(d.keys()))
    r = {}
    if key in all_keys:
        vals = set(filter(None, [d.get(key) for d in dicts]))
        r[key] = sep.join(sorted(list(vals)))
    return r


def migrate_field(obj, from_field, to_field):
    # todo - check for key existence
    old_val = obj[from_field]
    new_val = obj[to_field]
    del obj[from_field]

    if old_val is not None and new_val is not None:
        raise Exception("field migration clash, {}->{}".format(from_field, to_field))
    if old_val:
        obj[to_field] = old_val


def apply_license(archive_ingestion_date):
    if not archive_ingestion_date:
        return "notspecified"

    archive_ingestion_date = datetime.datetime.strptime(
        archive_ingestion_date, "%Y-%m-%d"
    ).date()

    if archive_ingestion_date + relativedelta(months=3) > datetime.date.today():
        return "other-closed"
    else:
        return apply_cc_by_license()


def apply_cc_by_license():
    return "CC-BY-4.0-AU"


def add_md5_from_stream_to_metadata(metadata, data):
    metadata.update({"md5": create_md5_from_stream(data)})


def create_md5_from_stream(data):
    return md5(data).hexdigest()


def get_md5_legacy_url(meta):
    first_md5_baseurl = [
        val.get("base_url")
        for key, val in meta.metadata_info.items()
        if key.endswith(".md5")
    ][0]
    return first_md5_baseurl


def clean_filename(filename):
    cleaned = bpa_munge_filename(filename)
    if filename != cleaned:
        logger.warn(f"Cleaned filename from '{filename}' to '{cleaned}'")
    return cleaned


logger = make_logger(__name__)
