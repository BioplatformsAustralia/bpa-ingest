import csv
import datetime
import json
import logging
import os
import re
import string
import tempfile
from collections import namedtuple
from hashlib import md5

import ckanapi
from dateutil.relativedelta import relativedelta

from bpaingest.libs.ingest_utils import ApiFqBuilder


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
        return "CC-BY-3.0-AU"


def resource_metadata_from_file(linkage, fname, resource_type):
    """
    the same XLSX file might be on multiple packages, so we generate an ID
    which is the MD5(str(linkage) || fname)
    """
    metadata = resource_metadata_from(linkage, fname, resource_type)
    with open(fname, "rb") as fd:
        data = fd.read()
    add_md5_from_stream_to_resource_metadata(metadata, data)
    return metadata


def resource_metadata_from_file_no_data(linkage, filename, resource_type):
    metadata = resource_metadata_from(linkage, filename, resource_type)
    return metadata


def resource_metadata_from(linkage, filename, resource_type):
    return {
        "id": md5(
            (str(linkage) + "||" + os.path.basename(filename)).encode("utf8")
        ).hexdigest(),
        "name": os.path.basename(filename),
        "resource_type": resource_type,
    }


def add_md5_from_stream_to_resource_metadata(metadata, data):
    metadata.update({"md5": md5(data).hexdigest()})


def add_raw_to_packages(logger, args, state, data_type_meta):
    for data_type in state:
        resource_filename_to_match = getattr(
            data_type_meta[data_type], "_raw_resources_file_name", ""
        )
        # use resource_linkage to line up resource against package
        for next_package in state[data_type]["packages"]:
            linkage_tpl = tuple(
                next_package[t] for t in data_type_meta[data_type].resource_linkage
            )
            raw_resources_path = get_raw_resources_filename_full_path(
                logger,
                state[data_type]["resources"],
                resource_filename_to_match,
                linkage_tpl,
            )
            if raw_resources_path:
                next_raw_resources_data = next_package.pop("raw_resources", None)
                if not next_raw_resources_data:
                    raise Exception(
                        "A raw resource path has been created, but there are no raw resources to append."
                    )
                for next_raw_id, next_raw_value in next_raw_resources_data.items():
                    fetched_descriptors = ckan_get_from_dict(
                        logger, args, next_raw_value
                    )
                    next_raw_value.update(fetched_descriptors)
                with open(raw_resources_path, "w") as raw_resources_file:
                    json.dump(
                        next_raw_resources_data,
                        raw_resources_file,
                        sort_keys=True,
                        indent=2,
                    )


def get_raw_resources_filename_full_path(
    logger, resources, resource_filename_to_match, linkage_tpl
):
    for resource_linkage, legacy_url, resource_obj in resources:
        if (
            resource_linkage == linkage_tpl
            and os.path.basename(legacy_url) == resource_filename_to_match
        ):
            return legacy_url


def ckan_get_from_dict(logger, ckan, dict):
    fq = ApiFqBuilder.from_collection(logger, dict)
    ## keep search parameters as broad as possible (the raw metadata may be from different project/organization
    #     # ckan api will only return first 1000 responses for some calls - so set very high limit.
    #     # Ensure that 'private' is turned on
    search_package_arguments = {
        "rows": 10000,
        "start": 0,
        "fq": fq,
        "include_private": True,
    }
    ckan_result = {}
    try:
        ckan_wrapped_results = ckan.call_action(
            "package_search", search_package_arguments
        )
        if ckan_wrapped_results and ckan_wrapped_results["count"] == 1:
            result = ckan_wrapped_results["results"][0]
            ckan_result = {"package_id": result["id"]}
        else:
            raise Exception(
                f"Unable to retrieve single result for raw package search. Unfortunately, the solr query: {fq} returned {getattr(ckan_wrapped_results, 'count', 0)} results."
            )
    except Exception as e:
        logger.error(e)
    finally:
        return ckan_result
