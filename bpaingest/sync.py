import re

from .ops import (
    ckan_method,
    patch_if_required,
    check_resource,
    create_resource,
    reupload_resource,
    get_organization,
    CKANArchiveInfo,
    ApacheArchiveInfo,
)
from .pkgcache import build_package_cache
import ckanapi

from .resource_metadata import (
    build_raw_resources_as_file,
    validate_raw_resources_file_metadata,
)
from .util import make_logger
from .util import prune_dict
from .libs.multihash import S3_HASH_FIELDS
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
        }
        ckan_obj = ckan_method(ckan, "package", "create")(**create_obj)
        logger.info("created package object: %s" % (obj["id"]))
    return ckan_obj


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
            ckan_method(ckan, "package", "delete")(id=delete_id)
            logger.info("deleted package: %s/%s" % (delete_obj["id"], delete_id))


def sync_packages(ckan, ckan_data_type, packages, org, group, do_delete):
    # FIXME: we don't check if there are any packages we should remove (unpublish)
    logger.info("syncing %d packages" % (len(packages)))
    # we have to post the group back in package objects, send a minimal version of it
    api_group_obj = prune_dict(
        group,
        ("display_name", "description", "title", "image_display_url", "id", "name"),
    )
    ckan_packages = []

    cache = build_package_cache(ckan, ckan_data_type, packages)

    delete_dangling_packages(ckan, packages, cache, do_delete)

    for package in sorted(packages, key=lambda p: p["name"]):
        obj = package.copy()
        obj["owner_org"] = org["id"]
        if api_group_obj is not None:
            obj["groups"] = [api_group_obj]
        ckan_packages.append(sync_package(ckan, obj, cache.get(obj["id"])))
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


def check_package_resources(ckan, ckan_packages, resource_id_legacy_url, auth):
    all_resources = []
    for package_obj in sorted(ckan_packages, key=lambda p: p["name"]):
        current_resources = package_obj["resources"]
        all_resources += current_resources

    return check_resources(ckan, all_resources, resource_id_legacy_url, auth, 8)


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
            logger.info("created resource: %s/%s" % (create_obj["package_id"], obj_id))
            to_reupload.append((current_ckan_obj, legacy_url))

    for obj_id in to_delete:
        delete_obj = existing_resources[obj_id]
        logger.info(
            "resource for deletion: %s/%s (do_delete=%s)"
            % (delete_obj["package_id"], obj_id, do_delete)
        )
        if do_delete:
            ckan_method(ckan, "resource", "delete")(id=obj_id)
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


def reupload_resources(ckan, to_reupload, resource_id_legacy_url, auth, num_threads):
    logger.info("%d objects to be re-uploaded" % (len(to_reupload)))
    destination = "bpa-ckan-prod/prodenv"
    if re.search("staging.bioplatforms", getattr(ckan, "address", "")):
        destination = "bpa-ckan-devel/staging"
    logger.info("Resources will be reuploaded under: {}".format(destination))
    for reupload_obj, legacy_url in to_reupload:
        reupload_resource(ckan, reupload_obj, legacy_url, destination, auth)


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
):
    logger.info("syncing %d resources" % (len(resources)))

    resource_linkage_package_id = {}
    for package_obj in ckan_packages:
        linkage_tpl = tuple((package_obj[t] for t in resource_linkage_attrs),)
        if linkage_tpl in resource_linkage_package_id:
            raise Exception(
                "more than one package linked for tuple {}".format(linkage_tpl)
            )
        resource_linkage_package_id[linkage_tpl] = package_obj["id"]

    # wire the resources to their CKAN package
    resource_idx = {}
    resource_id_legacy_url = {}
    for resource_linkage, legacy_url, resource_obj in resources:
        package_id = resource_linkage_package_id.get(resource_linkage)
        if package_id is None:
            logger.critical(
                "Unable to find package for `%s', skipping resource (%s)"
                % (repr(resource_linkage), legacy_url)
            )
        obj = resource_obj.copy()
        obj["package_id"] = package_id
        if package_id not in resource_idx:
            resource_idx[package_id] = []
        resource_idx[package_id].append(obj)
        if obj["id"] in resource_id_legacy_url:
            raise Exception("duplicate resource ID: {}".format(obj["id"]))
        resource_id_legacy_url[obj["id"]] = legacy_url

    if not do_resource_checks:
        logger.warning(
            "resource checks disabled: resource integrity will not be confirmed"
        )
        to_reupload = []
    else:
        # check all existing resources on all existing packages, in parallel
        to_reupload = check_package_resources(
            ckan, ckan_packages, resource_id_legacy_url, auth
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

    if do_uploads:
        reupload_resources(ckan, to_reupload, resource_id_legacy_url, auth, num_threads)


def sync_metadata(
    ckan, meta, auth, num_threads, do_uploads, do_resource_checks, do_delete
):
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

    organization = get_organization(ckan, meta.organization)
    packages = meta.get_packages()
    packages = list(unique_packages())
    resources = meta.get_resources()
    raw_resources_metadata = build_raw_resources_as_file(
        logger, ckan, meta, packages, resources
    )
    validate_raw_resources_file_metadata(logger, raw_resources_metadata, auth)
    ckan_packages = sync_packages(
        ckan, meta.ckan_data_type, packages, organization, None, do_delete
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
    )
