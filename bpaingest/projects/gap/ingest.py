

from unipath import Path
from collections import defaultdict

from ...abstract import BaseMetadata

from ...util import make_logger, sample_id_to_ckan_name, common_values
from urllib.parse import urljoin

from glob import glob

from ...libs import ingest_utils
from sslh.handler import SensitiveDataGeneraliser
from ...libs.excel_wrapper import make_field_definition as fld, SkipColumn as skp
from . import files
from .tracking import GAPTrackMetadata
from .contextual import (GAPSampleContextual)
from ...libs.ingest_utils import get_clean_number

import os
import re

logger = make_logger(__name__)
common_context = [GAPSampleContextual]


class GAPIlluminaShortreadMetadata(BaseMetadata):
    """
    this data conforms to the BPA 10X raw workflow. future data
    will use this ingest class.
    """
    organization = 'bpa-'
    ckan_data_type = 'gap-illumni-shortread'
    technology = 'illumina-shortread'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/plants_staging/genomics-illumina-shortread/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_sample_id', 'flow_id')
    spreadsheet = {
        'fields': [
            fld('sample_id', 'plant sample unique id', coerce=ingest_utils.extract_ands_id),
            fld('library_id', 'library id', coerce=ingest_utils.extract_ands_id),
            fld('dataset_id', 'dataset id', coerce=ingest_utils.extract_ands_id),
            fld('library_construction_protocol', 'library construction protocol'),
            fld('sequencer', 'sequencer'),
            fld('analysissoftwareversion', 'analysissoftwareversion'),
        ],
        'options': {
            'sheet_name': None,
            'header_length': 2,
            'column_name_row_index': 1,
        }
    }
    md5 = {
        'match': [
            files.illumina_shortread_re
        ],
        'skip': [
            re.compile(r'^.*_metadata\.xlsx$'),
            re.compile(r'^.*SampleSheet.*'),
            re.compile(r'^.*TestFiles\.exe.*'),
        ]
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super().__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = GAPTrackMetadata()

    def _get_packages(self):
        logger.info("Ingesting GAP metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing GAP metadata file {0}".format(fname))
            rows = self.parse_spreadsheet(fname, self.metadata_info)
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                sample_id = row.sample_id
                obj = row._asdict()
                obj.update(track_meta._asdict())
                name = sample_id_to_ckan_name(sample_id.split('/')[-1], self.ckan_data_type)
                obj.update({
                    'name': name,
                    'id': name,
                    'type': self.ckan_data_type,
                    'private': True,
                    'data_generated': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(sample_id, track_meta))
                tag_names = []
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                sample_id = ingest_utils.extract_ands_id(file_info.get('sample_id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((sample_id,), legacy_url, resource))
        return resources
