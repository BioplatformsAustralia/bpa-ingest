from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from .metadata import parse_metadata
from .samples import samples_from_metadata
from .files import files_from_md5

logger = make_logger(__name__)


class GbrAmpliconMetadata(object):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/gbr/metadata/amplicons/'
    auth = ("bpa", "gbr")

    def __init__(self, metadata_path):
        path = Path(metadata_path)
        self.metadata = parse_metadata(path)
        self.files = files_from_md5(path)

    def get_group(self):
        return {
            'name': 'great_barrier_reef',
            'title': 'Great Barrier Reef',
            'display_name': 'Great Barrier Reef',
            'image_url': 'https://downloads.bioplatforms.com/static/gbr/coral.png',
        }

    def get_packages(self):
        packages = []
        for bpa_id, data in samples_from_metadata(self.metadata).items():
            name = bpa_id_to_ckan_name(bpa_id)
            obj = data.copy()
            obj.update({
                'name': name,
                'id': bpa_id,
                'bpa_id': bpa_id,
                'title': 'Amplicon {}'.format(bpa_id),
                'notes': 'Amplicon Data for Great Barrier Reef Sample {}'.format(bpa_id),
                # 'tags': [{'Amplicon': data['amplicon']}],
                'type': 'great-barrier-reef-amplicon',
            })
            packages.append(obj)
        return packages

    def get_resources(self):
        resources = []
        for bpa_id, file_obj in self.files:
            legacy_url, resource = ckan_resource_from_file(file_obj)
            resources.append((bpa_id, legacy_url, resource))
        return resources


def ckan_resource_from_file(file_obj):
    ckan_obj = file_obj.copy()
    url = bpa_mirror_url('gbr/amplicons/{}/{}'.format(file_obj['amplicon'].lower(), file_obj['filename']))
    ckan_obj.update({
        'id': file_obj['md5']
    })
    return url, ckan_obj
