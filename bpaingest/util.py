import csv
import datetime
import logging
import os
import re
import string
from collections import namedtuple
from hashlib import md5

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
            "To use cache reuploads write interval, the interval must be an integer and cache write reuploads must be enabled."
        )
    logger.info(f"Activated write reuploads interval of {args.write_reuploads_interval}")
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
        if s[0] in string.digits:
            s = digit_words[s[0]] + s[1:]
        s = s.strip("_")
        s = re.sub(r"__+", "_", s).strip("_")
        # reserved words aren't permitted
        if s == "class":
            s = "class_"
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
        header = [name_fn(clean_name(t)) for t in next(r)] + additional_keys
        typ = namedtuple(typname, header)
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


def apply_license(archive_ingestion_date):
    if not archive_ingestion_date:
        return "notspecified"

    archive_ingestion_date = datetime.datetime.strptime(
        archive_ingestion_date, "%Y-%m-%d"
    ).date()

    if archive_ingestion_date + relativedelta(months=3) > datetime.date.today():
        return "other-closed"
    else:
        return "CC-BY-4.0-AU"


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
