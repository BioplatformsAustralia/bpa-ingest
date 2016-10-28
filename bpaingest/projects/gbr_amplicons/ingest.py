from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from .metadata import parse_metadata as amplicons_parse_metadata
from .samples import samples_from_metadata
from .files import files_from_md5
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class GbrAmpliconsMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/gbr/metadata/amplicons/']
    organization = 'bpa-great-barrier-reef'
    auth = ("bpa", "gbr")

    def __init__(self, metadata_path, track_csv_path=None):
        self.path = Path(metadata_path)
        self.files = files_from_md5(self.path)

    def get_packages(self):
        packages = []
        packages += list(self.amplicon_packages())
        return packages

    def get_resources(self):
        resources = []
        for bpa_id, file_obj in self.files:
            legacy_url, resource = ckan_resource_from_file(file_obj)
            resources.append((bpa_id, legacy_url, resource))
        return resources

    def amplicon_packages(self):
        metadata = amplicons_parse_metadata(self.path)
        for bpa_id, data in samples_from_metadata(metadata).items():
            name = bpa_id_to_ckan_name(bpa_id)
            obj = data.copy()
            obj.update({
                'name': name,
                'id': bpa_id,
                'bpa_id': bpa_id,
                'title': 'Amplicon {}'.format(bpa_id),
                'notes': 'Amplicon Data for Great Barrier Reef Sample {}'.format(bpa_id),
                'tags': [{'name': 'Amplicon'}],
                'type': 'great-barrier-reef-amplicon',
                'private': True,
            })
            yield obj


def ckan_resource_from_file(file_obj):
    ckan_obj = file_obj.copy()
    url = bpa_mirror_url('gbr/amplicons/{}/{}'.format(file_obj['amplicon'].lower(), file_obj['filename']))
    ckan_obj.update({
        'id': file_obj['md5']
    })
    return url, ckan_obj
