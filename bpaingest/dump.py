import json
import os
from collections import defaultdict
from .projects import PROJECTS
from .metadata import DownloadMetadata
from .util import make_logger


logger = make_logger(__name__)


def dump_state(args):
    state = defaultdict(lambda: defaultdict(list))

    # download metadata for all project types and aggregate metadata keys
    for project_name, project_cls in sorted(PROJECTS.items()):
        logger.info("Dumping state generation: %s / %s" % (project_name, project_cls.__name__))
        dlpath = os.path.join(args.download_path, project_cls.__name__)
        with DownloadMetadata(project_cls, path=dlpath) as dlmeta:
            meta = dlmeta.meta
            data_type = meta.ckan_data_type
            state[data_type]['packages'] += meta.get_packages()
            state[data_type]['resources'] += meta.get_resources()

    for data_type in state:
        state[data_type]['packages'].sort(key=lambda x: x['id'])
        state[data_type]['resources'].sort(key=lambda x: x[2]['id'])

    with open(args.filename, 'w') as fd:
        json.dump(state, fd, sort_keys=True, indent=2, separators=(',', ': '))
