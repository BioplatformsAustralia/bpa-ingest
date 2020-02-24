import json
import os
import re
from collections import defaultdict
from .projects import ProjectInfo
from .metadata import DownloadMetadata
from .util import make_logger


logger = make_logger(__name__)


def linkage_qc(state, data_type_meta):
    counts = {}

    # QC resource linkage
    for data_type in state:
        resource_linkage_package_id = {}

        packages = state[data_type]['packages']
        resources = state[data_type]['resources']
        counts[data_type] = len(packages), len(resources)

        for package_obj in packages:
            linkage_tpl = tuple(package_obj[t] for t in data_type_meta[data_type].resource_linkage)
            if linkage_tpl in resource_linkage_package_id:
                logger.error("{}: more than one package linked for tuple {}".format(data_type, linkage_tpl))
            resource_linkage_package_id[linkage_tpl] = package_obj['id']

        linked_tuples = set()
        for resource_linkage, legacy_url, resource_obj in resources:
            linked_tuples.add(resource_linkage)
            if resource_linkage not in resource_linkage_package_id:
                logger.error("{}: dangling resource: {}".format(data_type, resource_linkage))

        for linkage_tpl, package_id in resource_linkage_package_id.items():
            if linkage_tpl not in linked_tuples:
                logger.error("{}: package has no linked resources, tuple: {}".format(
                    package_id,
                    linkage_tpl))

    for data_type, (p, r) in counts.items():
        logger.debug("{}: {} packages, {} resources".format(data_type, p, r))


def dump_state(args):
    state = defaultdict(lambda: defaultdict(list))

    project_info = ProjectInfo()
    classes = sorted(project_info.metadata_info, key=lambda t: t['slug'])
    if args.dump_re:
        r = re.compile(args.dump_re, re.IGNORECASE)
        new_classes = list(
            filter(lambda x: r.match(x['slug']), classes))
        if len(new_classes) == 0:
            logger.error('No matches, possibilities: {}'.format([t['slug'] for t in classes]))
        classes = new_classes
    logger.info('dumping: {}'.format(', '.join(t['slug'] for t in classes)))

    data_type_meta = {}
    # download metadata for all project types and aggregate metadata keys
    for class_info in classes:
        logger.info("Dumping state generation: %s / %s" % (class_info['project'], class_info['slug']))
        dlpath = os.path.join(args.download_path, class_info['slug'])
        with DownloadMetadata(class_info['cls'], path=dlpath) as dlmeta:
            meta = dlmeta.meta
            data_type = meta.ckan_data_type
            data_type_meta[data_type] = meta
            state[data_type]['packages'] += meta.get_packages()
            state[data_type]['resources'] += meta.get_resources()

    for data_type in state:
        state[data_type]['packages'].sort(key=lambda x: x['id'])
        state[data_type]['resources'].sort(key=lambda x: x[2]['id'])

    linkage_qc(state, data_type_meta)

    with open(args.filename, 'w') as fd:
        json.dump(state, fd, sort_keys=True, indent=2, separators=(',', ': '))
