import json
import os
import re
from collections import defaultdict, Counter

from .metadata import DownloadMetadata
from .projects import ProjectInfo
from .resource_metadata import (
    build_raw_resources_from_state_as_file,
    validate_raw_resources_from_state,
)
from .util import make_logger, make_ckan_api


def unique_packages(logger, packages):
    by_id = dict((t["id"], t) for t in packages)
    id_count = Counter(t["id"] for t in packages)
    for k, cnt in list(id_count.items()):
        if cnt > 1:
            dupes = [t for t in packages if t["id"] == k]
            logger.critical(
                "package id `%s' appears %d times: excluded from sync" % (k, len(dupes))
            )
            continue
        yield by_id[k]


def linkage_qc(logger, state, data_type_meta, errors_callback=None):
    if not errors_callback:
        errors_callback = logger.error
    counts = {}

    # QC resource linkage
    for data_type in state:
        resource_linkage_package_id = {}

        packages = list(unique_packages(logger, (state[data_type]["packages"])))
        resources = state[data_type]["resources"]
        counts[data_type] = len(packages), len(resources)

        for package_obj in packages:
            linkage_tpl = tuple(
                package_obj[t] for t in data_type_meta[data_type].resource_linkage
            )
            if linkage_tpl in resource_linkage_package_id:
                errors_callback(
                    "{}: more than one package linked for tuple {}".format(
                        data_type, linkage_tpl
                    )
                )
            resource_linkage_package_id[linkage_tpl] = package_obj["id"]

        linked_tuples = set()
        for resource_linkage, legacy_url, resource_obj in resources:
            linked_tuples.add(resource_linkage)
            if resource_linkage not in resource_linkage_package_id:
                dirname1, resource_name = os.path.split(legacy_url)
                _dirname2, ticket = os.path.split(dirname1)
                errors_callback(
                    "dangling resource: name `{}' (ticket: `{}', linkage: `{}')".format(
                        resource_name, ticket, resource_linkage
                    )
                )

        for linkage_tpl, package_id in resource_linkage_package_id.items():
            if linkage_tpl not in linked_tuples:
                errors_callback(
                    "{}: package has no linked resources, tuple: {}".format(
                        package_id, linkage_tpl
                    )
                )

    for data_type, (p, r) in counts.items():
        logger.info("{}: {} packages, {} resources".format(data_type, p, r))


def dump_state(args):
    if args.log_level:
        logger = make_logger(__name__, args.log_level)
    else:
        logger = make_logger(__name__)
    state = defaultdict(lambda: defaultdict(list))

    project_info = ProjectInfo()
    classes = sorted(project_info.metadata_info, key=lambda t: t["slug"])
    if args.dump_re:
        r = re.compile(args.dump_re, re.IGNORECASE)
        new_classes = list(filter(lambda x: r.match(x["slug"]), classes))
        if len(new_classes) == 0:
            logger.error(
                "No matches, possibilities:\n{}".format(
                    "\n".join([t["slug"] for t in classes])
                )
            )
        classes = new_classes
    logger.info("dumping: {}".format(", ".join(t["slug"] for t in classes)))
    has_sql_context = True if args.sql_context == "True" else False

    data_type_meta = {}
    # download metadata for all project types and aggregate metadata keys
    for class_info in sorted(classes, key=lambda x: x["slug"]):
        logger.info(
            "Dumping state generation: %s / %s"
            % (class_info["project"], class_info["slug"])
        )
        dlpath = os.path.join(args.download_path, class_info["slug"])
        with DownloadMetadata(
            make_logger(class_info["slug"]),
            class_info["cls"],
            path=dlpath,
            has_sql_context=has_sql_context,
        ) as dlmeta:
            meta = dlmeta.meta
            data_type = meta.ckan_data_type
            data_type_meta[data_type] = meta
            state[data_type]["packages"] += meta.get_packages()
            state[data_type]["resources"] += meta.get_resources()
            state[data_type]["auth"] = dlmeta.auth

    for data_type in state:
        state[data_type]["packages"].sort(key=lambda x: x["id"])
        state[data_type]["resources"].sort(key=lambda x: x[2]["id"])

    linkage_qc(logger, state, data_type_meta)

    ckan = make_ckan_api(args)

    build_raw_resources_from_state_as_file(logger, ckan, state, data_type_meta)
    validate_raw_resources_from_state(logger, state)

    # for datetime objects, use 'default as str' for now so that parsing doesn't break
    with open(args.filename, "w") as fd:
        json.dump(state, fd, sort_keys=True, indent=2, separators=(",", ": "))
