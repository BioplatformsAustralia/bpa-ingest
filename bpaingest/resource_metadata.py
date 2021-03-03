import json
import os
import re
import urllib
from hashlib import md5

from bpaingest.ops import ckan_get_from_dict, download_legacy_file
from bpaingest.util import (
    create_md5_from_stream,
    add_md5_from_stream_to_metadata,
    get_md5_legacy_url,
)


def resource_metadata_from_file(linkage, fname, resource_type):
    """
    the same XLSX file might be on multiple packages, so we generate an ID
    which is the MD5(str(linkage) || fname)
    """
    metadata = resource_metadata_from(linkage, fname, resource_type)
    with open(fname, "rb") as fd:
        data = fd.read()
    add_md5_from_stream_to_metadata(metadata, data)
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


def validate_raw_resources_from_state(logger, state):
    for data_type in state:
        validate_raw_resources_file_metadata(
            logger, state[data_type]["raw_resources_files"], state[data_type]["auth"]
        )


def validate_raw_resources_file_metadata(logger, raw_resources_metadata, auth):
    base_validation_action_message = "Raw resources must be copied up to the remote server, before running this ingest again."
    for next in raw_resources_metadata:
        legacy_url = next["metadata"][1]
        if re.search(r"^file.*", legacy_url, re.VERBOSE):
            logger.info(
                f"Validation of generated raw resources aborted as remote URL is a local file. {base_validation_action_message}"
            )
            return
        logger.info(
            f"Checking MD5 on raw resources file, {next['path']}  against remote URL: {next['metadata'][1]}"
        )
        raw_resource_md5 = next["metadata"][2]["md5"]
        logger.debug(f"raw resource md5 is: {raw_resource_md5}")
        tempdir, path = download_legacy_file(legacy_url, auth)
        logger.debug(f"path is {path}")
        with open(path, "rb") as fd:
            # ensure no newlines read in from end of file so that it matches validation of earlier json dump of resource metadata
            data = fd.read().rstrip()
            md5 = create_md5_from_stream(data)
            logger.debug(f"md5 from stream of download URL is: {md5}")
            if md5 != raw_resource_md5:
                raise Exception(
                    f"The md5sum of raw resources content does not match the content on remote downloads server. {base_validation_action_message}"
                )


def build_raw_resources_from_state_as_file(logger, ckan, state, data_type_meta):
    for data_type in state:
        state[data_type]["raw_resources_files"] = build_raw_resources_as_file(
            logger,
            ckan,
            data_type_meta[data_type],
            state[data_type]["packages"],
            state[data_type]["resources"],
        )


def build_raw_resources_as_file(logger, ckan, meta, packages, resources):
    raw_resources_files = []
    raw_resources_linkage = getattr(meta, "_raw_resources_linkage", "")
    if raw_resources_linkage:
        # use resource_linkage to line up resource against package
        for next_package in packages:
            linkage_tpl = tuple(next_package[t] for t in meta.resource_linkage)
            raw_resources_path = get_raw_resources_filename_full_path(
                raw_resources_linkage, linkage_tpl
            )
            raw_resources_metadata = get_raw_resources_metadata(
                resources, linkage_tpl, raw_resources_path
            )

            next_raw_resources_data = next_package.pop("raw_resources", None)
            if not next_raw_resources_data:
                raise Exception(
                    "A raw resource path has been created, but there are no raw resources to append."
                )
            for next_raw_id, next_raw_value in next_raw_resources_data.items():
                fetched_descriptors = ckan_get_from_dict(logger, ckan, next_raw_value)
                next_raw_value.update(fetched_descriptors)
            with open(raw_resources_path, "w") as raw_resources_file:
                json.dump(
                    next_raw_resources_data,
                    raw_resources_file,
                    sort_keys=True,
                    indent=2,
                )
            update_raw_resources_metadata(resources, linkage_tpl, raw_resources_path)
            raw_resources_files.append(
                {"path": raw_resources_path, "metadata": raw_resources_metadata}
            )
            logger.info(f"Generated raw resouces file: {raw_resources_path}.")
        md5_legacy_url = get_md5_legacy_url(meta)
        logger.warning(
            f"Any updates to generated raw resources files, need to be copied to remote download server at: {md5_legacy_url} "
        )
    return raw_resources_files


def get_raw_resources_filename_full_path(raw_resources_linkages, linkage_tpl):
    filepath = raw_resources_linkages[linkage_tpl]
    return urllib.parse.urlparse(filepath).path


def get_raw_resources_metadata(resources, linkage_tpl, full_path_name):
    raw_filename = os.path.basename(full_path_name)
    for resource_linkage, legacy_url, resource_obj in resources:
        if resource_linkage == linkage_tpl and raw_filename == resource_obj.get(
            "name", ""
        ):
            return (resource_linkage, legacy_url, resource_obj)


def update_raw_resources_metadata(resources, linkage_tpl, full_path_name):
    raw_filename = os.path.basename(full_path_name)
    for resource_linkage, legacy_url, resource_obj in resources:
        if resource_linkage == linkage_tpl and raw_filename == resource_obj.get(
            "name", ""
        ):
            with open(full_path_name, "rb") as fd:
                data = fd.read()
                add_md5_from_stream_to_metadata(resource_obj, data)
