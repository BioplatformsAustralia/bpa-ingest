import os
import pickle
import re

from bpaingest.ops import (
    ckan_method,
    patch_if_required,
    check_resource,
    create_resource,
    reupload_resource,
    get_organization,
    make_organization,
    CKANArchiveInfo,
    ApacheArchiveInfo,
)
from bpaingest.pkgcache import build_package_cache
import ckanapi
import botocore

from bpaingest.resource_metadata import (
    build_raw_resources_as_file,
    validate_raw_resources_file_metadata,
)
from bpaingest.util import make_logger
from bpaingest.util import prune_dict
from bpaingest.libs.multihash import S3_HASH_FIELDS
from bpaingest.libs.bpa_constants import AUDIT_DELETED, AUDIT_VERIFIED
from bpaingest.libs.s3 import merge_and_update_tags
from collections import Counter

logger = make_logger(__name__)


def get_or_create_package(ckan, obj):
    try:
        ckan_obj = ckan_method(ckan, "package", "show")(id=obj["name"])
        if ckan_obj["state"] == "deleted":
            logger.info(
                "found %s package object, purging: %s"
                % (ckan_obj["state"], ckan_obj["id"])
            )
            # purge deleted dataset to allow creation of new package
            ckan_method(ckan, "dataset", "purge")(id=ckan_obj["id"])

            # deleted and purged things shouldn't be found, simulate error
            raise ckanapi.errors.NotFound
    except ckanapi.errors.NotFound:
        create_obj = {
            "type": obj["type"],
            "id": obj["id"],
            "name": obj["name"],
            "owner_org": obj["owner_org"],
            "private": obj["private"],
            "resource_permissions": obj["resource_permissions"],
            "access_control_reason": obj["access_control_reason"],
            "access_control_date": obj["access_control_date"],
            "access_control_mode": obj["access_control_mode"],
        }
        ckan_obj = ckan_method(ckan, "package", "create")(**create_obj)
        logger.info("created package object: %s" % (obj["id"]))
    return ckan_obj


def get_or_create_resource(ckan, obj):
    try:
        ckan_obj = ckan_method(ckan, "resource", "show")(id=obj["id"])

    except ckanapi.errors.NotFound:
        ckan_obj = ckan_method(ckan, "resource", "create")(**obj)
        logger.info("created resource object: %s" % (ckan_obj["id"]))
    return ckan_obj


def get_uploaded_resource_from_ckan(ckan, obj):
    #  this will return None if the resource is NOT uploaded to S3.
    try:
        resource_from_ckan = ckan_method(ckan, "resource", "show")(id=obj["id"])
    except ckanapi.errors.NotFound:
        return None
    if (
        "url_type" not in resource_from_ckan
        or resource_from_ckan["url_type"] != "upload"
    ):
        # it doesn't have an uploaded file we want to track, return None
        return None
    return resource_from_ckan


def sync_package(ckan, obj, cached_obj):
    if cached_obj is None:
        ckan_obj = get_or_create_package(ckan, obj)
    else:
        ckan_obj = cached_obj
    patch_obj = obj.copy()
    patch_obj["id"] = ckan_obj["id"]
    # tags are handed back with a bunch of info that's irrelevant
    compare_ckan_obj = ckan_obj.copy()
    compare_ckan_obj["tags"] = [{"name": t["name"]} for t in ckan_obj["tags"]]
    was_patched, ckan_obj = patch_if_required(
        ckan, "package", compare_ckan_obj, patch_obj
    )
    if was_patched:
        logger.info("patched package object: %s" % (obj["id"]))
    return ckan_obj


def audit_resource(audit_tag, description, ckan, delete_id, resource_obj):
    # Update s3tags for 'audit' to be AUDIT_DELETED
    s3_tags = {
        "audit": audit_tag,
    }

    filename = resource_obj["name"]
    destination = determine_destination(ckan)
    bucket = destination.split("/")[0]
    key = "{}/resources/{}/{}".format(
        destination.split("/", 1)[1], resource_obj["id"], filename
    )

    logger.info(
        "Tagging %s resource object: %s"
        % (
            description,
            key,
        )
    )

    try:
        merge_and_update_tags(bucket, key, s3_tags)
    except botocore.errorfactory.NoSuchKey:
        logger.critical(
            "Unable to object with key `%s', for resource object (%s)"
            % (key, repr(resource_obj))
        )
        raise Exception("Unable to tag S3 Resource due to error")


def tag_verified_resource(ckan, verified_id, resource_obj):
    audit_tag = AUDIT_VERIFIED
    description = "verified"
    audit_resource(audit_tag, description, ckan, verified_id, resource_obj)


def tag_deleted_resource(ckan, delete_id, resource_obj):
    audit_tag = AUDIT_DELETED
    description = "deleted"
    audit_resource(audit_tag, description, ckan, delete_id, resource_obj)


def delete_resource(ckan, delete_id, resource_obj):
    tag_deleted_resource(ckan, delete_id, resource_obj)
    ckan_method(ckan, "resource", "delete")(id=delete_id)


def delete_package(ckan, delete_id, package_obj):
    # delete_id and package_obj["id"] should be same / equivalent

    # iterate through packages resources and tag in s3 as deleted
    for resource in package_obj["resources"]:
        resource_obj = get_uploaded_resource_from_ckan(ckan, resource)
        if resource_obj:
            tag_deleted_resource(ckan, resource["id"], resource_obj)

    ckan_method(ckan, "package", "delete")(id=delete_id)


def delete_dangling_packages(ckan, packages, cache, do_delete):
    extant_ids = set(cache.keys())
    continuing_ids = set(t["id"] for t in packages)
    to_delete = extant_ids - continuing_ids

    for delete_id in to_delete:
        delete_obj = cache[delete_id]
        logger.info(
            "package for deletion: %s/%s (do_delete=%s)"
            % (delete_obj["id"], delete_id, do_delete)
        )
        if do_delete:
            delete_package(ckan, delete_id, delete_obj)
            logger.info("deleted package: %s/%s" % (delete_obj["id"], delete_id))


def sync_packages(
    ckan, ckan_data_type, packages, org, group, do_delete, do_single_ticket, do_audit
):
    # FIXME: we don't check if there are any packages we should remove (unpublish)
    logger.info("syncing %d packages" % (len(packages)))
    reporting_interval = determine_reporting_interval(len(packages))
    # we have to post the group back in package objects, send a minimal version of it
    api_group_obj = prune_dict(
        group,
        ("display_name", "description", "title", "image_display_url", "id", "name"),
    )
    ckan_packages = []

    cache = build_package_cache(ckan, ckan_data_type, packages)
    if do_single_ticket is None:  # no need to try to delete them
        delete_dangling_packages(ckan, packages, cache, do_delete)

    synched_package_count = 0
    for package in sorted(packages, key=lambda p: p["name"]):
        obj = package.copy()
        obj["owner_org"] = org["id"]
        if api_group_obj is not None:
            obj["groups"] = [api_group_obj]
        if do_single_ticket is None or obj["ticket"] == do_single_ticket:
            ckan_packages.append(sync_package(ckan, obj, cache.get(obj["id"])))
            synched_package_count += 1
            if synched_package_count % reporting_interval == 0:
                logger.info(
                    "synced %d of %d packages" % (synched_package_count, len(packages))
                )
    return ckan_packages


def check_resources(ckan, current_resources, resource_id_legacy_url, auth, num_threads):
    ckan_archive_info = CKANArchiveInfo(ckan)
    apache_archive_info = ApacheArchiveInfo(auth)
    to_reupload = []

    def check(current_ckan_obj, legacy_url, current_url):
        obj_id = current_ckan_obj["id"]
        resource_issue = check_resource(
            ckan_archive_info,
            apache_archive_info,
            current_url,
            legacy_url,
            [current_ckan_obj.get(t) for t in S3_HASH_FIELDS],
        )
        if resource_issue:
            logger.error(
                "resource check failed (%s) queued for re-upload: %s"
                % (resource_issue, obj_id)
            )
            to_reupload.append((current_ckan_obj, legacy_url))
        else:
            logger.info("resource check OK: %s" % (obj_id))

    logger.info("%d resources to be checked" % (len(current_resources)))
    for current_ckan_obj in current_resources:
        obj_id = current_ckan_obj["id"]
        legacy_url = resource_id_legacy_url.get(obj_id)
        current_url = current_ckan_obj.get("url")
        check(current_ckan_obj, legacy_url, current_url)

    return to_reupload


def audit_resources(ckan, current_resources):
    logger.info("%d resources to be audited" % (len(current_resources)))
    for current_ckan_obj in current_resources:
        obj_id = current_ckan_obj["id"]
        tag_verified_resource(ckan, obj_id, current_ckan_obj)


def check_package_resources(ckan, ckan_packages, resource_id_legacy_url, auth):
    all_resources = []
    for package_obj in sorted(ckan_packages, key=lambda p: p["name"]):
        current_resources = package_obj["resources"]
        all_resources += current_resources

    return check_resources(ckan, all_resources, resource_id_legacy_url, auth, 8)


def audit_package_resources(ckan, ckan_packages):
    all_resources = []
    for package_obj in sorted(ckan_packages, key=lambda p: p["name"]):
        current_resources = package_obj["resources"]
        all_resources += current_resources

    return audit_resources(ckan, all_resources)


def sync_package_resources(
    ckan, package_obj, resource_id_legacy_url, resources, auth, do_delete
):
    current_resources = package_obj["resources"]
    existing_resources = dict((t["id"], t) for t in current_resources)
    needed_resources = dict((t["id"], t) for t in resources)

    if len(needed_resources) != len(resources):
        raise Exception(
            "duplicate MD5 hashes: {}".format(
                sorted(set(resources) - set(needed_resources))
            )
        )

    to_create = set(needed_resources) - set(existing_resources)
    to_delete = set(existing_resources) - set(needed_resources)

    to_reupload = []
    created_resource_count = 0
    uncreated_resource_count = 0
    for obj_id in to_create:
        resource_obj = needed_resources[obj_id]
        legacy_url = resource_id_legacy_url[obj_id]
        # we don't upload at the time we create the resource: it's more useful to immediately
        # get all the metadata into the CKAN instance, with links to legacy mirrors. we can
        # them come back and upload into CKAN using the reupload functionality of this script
        create_obj = resource_obj.copy()
        create_obj["url"] = legacy_url
        current_ckan_obj = create_resource(ckan, create_obj)
        if current_ckan_obj:
            created_resource_count += 1
            logger.info("created resource: %s/%s" % (create_obj["package_id"], obj_id))
            to_reupload.append((current_ckan_obj, legacy_url))
        else:
            uncreated_resource_count += 1
        # logger.info("Processing Resource creation: created %d, did not create %d of expected %d"
        #            % (created_resource_count, uncreated_resource_count, len(to_create)))

    for obj_id in to_delete:
        delete_obj = existing_resources[obj_id]
        logger.info(
            "resource for deletion: %s/%s (do_delete=%s)"
            % (delete_obj["package_id"], obj_id, do_delete)
        )
        if do_delete:
            delete_resource(ckan, obj_id, delete_obj)
            logger.info("deleted resource: %s/%s" % (delete_obj["package_id"], obj_id))

    # patch all the resources, to ensure everything is synced on
    # existing resources
    if to_create or to_delete:
        # if we've changed the resources attached to the package, refresh it
        package_obj = ckan_method(ckan, "package", "show")(id=package_obj["id"])
    current_resources = package_obj["resources"]
    for current_ckan_obj in current_resources:
        obj_id = current_ckan_obj["id"]
        resource_obj = needed_resources.get(obj_id)
        if resource_obj is None:
            logger.debug("skipping patch of unknown resource: {}".format(obj_id))
            continue
        legacy_url = resource_id_legacy_url[obj_id]
        was_patched, ckan_obj = patch_if_required(
            ckan, "resource", current_ckan_obj, resource_obj
        )
        if was_patched:
            logger.info("patched resource: %s" % (obj_id))

    return to_reupload


def determine_destination(ckan):
    # TODO: there is no bucket for anything other than prod OR STAGING - however it's unclear whether this breaks in non-prod environments
    ## Test this by setting it to None or '' any that way we don't accidentally send data to a bucket that is inadvertently created in S3
    destination = None

    if re.search("^https://data.bioplatforms.com", getattr(ckan, "address", "")):
        destination = "bpa-ckan-prod/prodenv"
        logger.info("Resources will be reuploaded under: {}".format(destination))
    elif re.search("^https://staging.bioplatforms.com", getattr(ckan, "address", "")):
        destination = "bpa-ckan-staging/stagingenv"
        logger.info("Resources will be reuploaded under: {}".format(destination))
    else:
        logger.warn(
            "Resources have no bucket to send to. Address was: {}".format(
                getattr(ckan, "address", "")
            )
        )

    return destination


def reupload_resources(
    ckan,
    to_reupload,
    shared_resources,
    auth,
    write_reuploads_fn,
    write_reuploads_interval,
):
    def do_actual_upload(ckan, reupload_obj, legacy_url, destination, auth):
        try:
            reupload_resource(ckan, reupload_obj, legacy_url, destination, auth)
        except Exception as e:
            logger.error(e)
            logger.info("Resource failed to upload. Continuing...")
        else:
            to_reupload.remove((reupload_obj, legacy_url))
            logger.info(
                f"Resource successfully uploaded. Removed {reupload_obj} at {legacy_url} from reupload list..."
            )
        finally:
            remaining_reuploads_count = len(to_reupload)
            logger.info(
                f"Resource Upload progress: {remaining_reuploads_count} out of {total_reuploads} to do."
            )
            # Only write to disk when interval counter reached
            if (
                write_reuploads_fn
                and remaining_reuploads_count % int(write_reuploads_interval) == 0
            ):
                logger.info(
                    f"Reached write reuploads interval: {write_reuploads_interval}"
                )
                write_reuploads_fn(to_reupload)

    total_reuploads = len(to_reupload)
    logger.info("The following files are ready to be re-uploaded:")
    for reupload_obj in to_reupload:
        logger.info(reupload_obj[0]["url"])
    logger.info("Total of %d objects to be re-uploaded" % (total_reuploads))
    destination = determine_destination(ckan)

    # copy list and loop that, so can remove safely from original during loop
    for indx, (reupload_obj, legacy_url) in enumerate(to_reupload[:]):
        # first determine if this is a shared file.
        # if it is, has it already been uploaded?
        # if NOT, go off and upload it, and capture the necessary fields to reuse in our shared files list.
        # if so, don't upload it again, but we do need to update the url, size etc from uploaded version of the resource
        # if its NOT a share resource, just upload as normal.
        if "shared_file" in reupload_obj and reupload_obj["shared_file"]:
            shared_linkage = reupload_obj["md5"] + "/" + reupload_obj["name"]
            if shared_linkage not in shared_resources:
                logger.error(
                    "No shared resource on file for {}, resource {},  not uploading".format(
                        shared_linkage, reupload_obj
                    )
                )
                continue
            uploaded_shared_resource = shared_resources[shared_linkage][0].get(
                "uploaded_resource"
            )
            if (
                uploaded_shared_resource is None
                or "size" not in uploaded_shared_resource
            ):
                # do the upload, and add the updated resource to the shared_resources object
                do_actual_upload(ckan, reupload_obj, legacy_url, destination, auth)
                ckan_updated_resource = get_or_create_resource(ckan, reupload_obj)
                shared_resources[shared_linkage][0] = {
                    "uploaded_resource": ckan_updated_resource
                }
            else:
                # just update the resource with the metadata from the matching shared_resource.
                reupload_obj["url"] = uploaded_shared_resource["url"]
                reupload_obj["size"] = uploaded_shared_resource["size"]
                reupload_obj["url_type"] = ""  # explicitly NOT upload
                ckan_method(ckan, "resource", "update")(**reupload_obj)
                to_reupload.remove((reupload_obj, legacy_url))
                logger.info(
                    f"Shared resource not re-uploaded. Removed {reupload_obj} at {legacy_url} from reupload list..."
                )

        else:  # it's not a shared file, upload regardless.
            do_actual_upload(ckan, reupload_obj, legacy_url, destination, auth)


def write_reuploads(**kwargs):
    if kwargs["write_reuploads"] and kwargs["reuploads_path"]:

        def dump_reload(to_reupload):
            with open(kwargs["reuploads_path"], "wb") as writer:
                pickle.dump(to_reupload, writer)
            logger.info(f"Reuploads disk cache write completed.")

        return dump_reload
    else:
        logger.info("Reuploads write disabled.")


def sync_resources(
    ckan,
    resources,
    resource_linkage_attrs,
    ckan_packages,
    auth,
    num_threads,
    do_uploads,
    do_resource_checks,
    do_delete,
    do_single_ticket,
    do_audit,
    **kwargs,
):
    logger.info("checking  %d resources for synch" % (len(resources)))
    reporting_interval = determine_reporting_interval(len(resources))
    resource_linkage_package_id = {}
    for package_obj in ckan_packages:
        linkage_tpl = tuple(
            (package_obj[t] for t in resource_linkage_attrs),
        )
        if linkage_tpl in resource_linkage_package_id:
            raise Exception(
                "more than one package linked for tuple {}".format(linkage_tpl)
            )
        resource_linkage_package_id[linkage_tpl] = package_obj["id"]
    resources_synched = 0
    # wire the resources to their CKAN package
    resource_idx = {}
    resource_id_legacy_url = {}
    shared_resources = {}
    for resource_linkage, legacy_url, resource_obj in resources:
        package_id = resource_linkage_package_id.get(resource_linkage)
        if do_single_ticket is None:
            if package_id is None:
                logger.critical(
                    "Unable to find package for `%s', skipping resource (%s)"
                    % (repr(resource_linkage), legacy_url)
                )
                continue
        else:
            if (
                package_id is None
            ):  # no package found for this resource, it does not belong to our target ticket.
                continue
        obj = resource_obj.copy()
        obj["package_id"] = package_id
        if package_id not in resource_idx:
            resource_idx[package_id] = []
        resource_idx[package_id].append(obj)
        if obj["id"] in resource_id_legacy_url:
            raise Exception("duplicate resource ID: {}".format(obj["id"]))
        resource_id_legacy_url[obj["id"]] = legacy_url
        # if this is a shared resource, and not already on the list of shared resources, add it in.
        # shared resource should be unique by md5 and filename, so use that as a key.
        if "shared_file" in obj and obj["shared_file"]:
            shared_linkage = obj["md5"] + "/" + obj["name"]
            # if we haven't seen this linkage before, see if there is already a resource uploaded in CKAN
            if (
                shared_linkage not in shared_resources
            ):  # we haven't seen this shared file before
                shared_resources.setdefault(shared_linkage, []).append(
                    {"uploaded_resource": get_uploaded_resource_from_ckan(ckan, obj)}
                )
            else:
                if shared_resources[shared_linkage][0].get("uploaded_resource") is None:
                    shared_resources[shared_linkage][0] = {
                        "uploaded_resource": get_uploaded_resource_from_ckan(ckan, obj)
                    }

        resources_synched += 1
        if resources_synched % reporting_interval == 0:
            logger.info(
                "synced %d of %d resources" % (resources_synched, len(resources))
            )

    if do_audit:
        logger.info("auditing all exising resources attached to packages")
        audit_package_resources(ckan, ckan_packages)

    if not do_resource_checks:
        logger.warning(
            "resource checks disabled: resource integrity will not be confirmed"
        )
        to_reupload = []
    elif kwargs["read_reuploads"]:
        with open(kwargs["reuploads_path"], "rb") as reader:
            to_reupload = pickle.load(reader)
        logger.info(f"Reuploads disk cache read completed.")
    else:
        # check all existing resources on all existing packages, in parallel
        to_reupload = check_package_resources(
            ckan, ckan_packages, resource_id_legacy_url, auth
        )

    logger.info(
        f"Before the package resources sync, reupload count is: {len(to_reupload)}"
    )
    for package_obj in sorted(ckan_packages, key=lambda p: p["name"]):
        package_id = package_obj["id"]
        package_resources = resource_idx.get(package_id)
        if package_resources is None:
            logger.warning("No resources for package `%s`" % (package_id))
            continue
        to_reupload += sync_package_resources(
            ckan,
            package_obj,
            resource_id_legacy_url,
            package_resources,
            auth,
            do_delete,
        )

    write_reuploads_fn = write_reuploads(**kwargs)
    if do_uploads:
        reupload_resources(
            ckan,
            to_reupload,
            shared_resources,
            auth,
            write_reuploads_fn,
            kwargs.get("write_reuploads_interval"),
        )

    logger.info(f"Post resource upload, resources remaining: {len(to_reupload)}")
    if write_reuploads_fn:
        write_reuploads_fn(to_reupload)


def sync_metadata(
    ckan,
    meta,
    auth,
    num_threads,
    do_uploads,
    do_resource_checks,
    do_delete,
    do_update_orgs,
    do_single_ticket,
    do_audit,
    **kwargs,
):
    # command line to update orgs as dev for plant pathogens:
    # bpa-ingest sync --skip-resource-checks --metadata-only --update-orgs --verify-ssl False -u https://localhost:8443
    #     -k [key goes here] pp-illumina-shortread
    def unique_packages():
        by_id = dict((t["id"], t) for t in packages)
        id_count = Counter(t["id"] for t in packages)
        for k, cnt in list(id_count.items()):
            if cnt > 1:
                dupes = [t for t in packages if t["id"] == k]
                logger.critical(
                    "package id `%s' appears %d times: excluded from sync"
                    % (k, len(dupes))
                )
                continue
            yield by_id[k]

    if do_update_orgs and hasattr(meta, "google_project_codes_meta"):
        sync_child_organizations(ckan, meta.google_project_codes_meta)
    organization = get_organization(ckan, meta.organization)
    packages = meta.get_packages()
    packages = list(unique_packages())
    if do_single_ticket is not None:
        ticket_packages = []
        for package in packages:
            if package["ticket"] == do_single_ticket:
                ticket_packages.append(package)
        packages = ticket_packages

    resources = meta.get_resources()

    raw_resources_metadata = build_raw_resources_as_file(
        logger, ckan, meta, packages, resources
    )
    validate_raw_resources_file_metadata(logger, raw_resources_metadata, auth)
    ckan_packages = sync_packages(
        ckan,
        meta.ckan_data_type,
        packages,
        organization,
        None,
        do_delete,
        do_single_ticket,
        do_audit,
    )
    sync_resources(
        ckan,
        resources,
        meta.resource_linkage,
        ckan_packages,
        auth,
        num_threads,
        do_uploads,
        do_resource_checks,
        do_delete,
        do_single_ticket,
        do_audit,
        **kwargs,
    )


def sync_child_organizations(ckan, project_info):
    parent_org = project_info.parent_org
    for row in project_info.project_code_rows:
        org = {
            "name": row.slug,
            "title": row.short_description,
            "description": row.long_description,
            "groups": [{"capacity": "public", "name": parent_org}],
            "extras": [{"key": "Private", "value": "True"}],
        }
        make_organization(ckan, org)


def determine_reporting_interval(total_count):
    # this method is used to calculate reporting of progress to a respectable level
    # if there are fewer than 100, you can report each one. If > 100, < 1000, report every 10%
    # if > 100, report every 1%
    reporting_interval = 1
    if total_count > 1000:
        reporting_interval = int(total_count / 100)
    else:
        if total_count > 100:
            reporting_interval = int(total_count / 10)
    return reporting_interval
