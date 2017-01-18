from __future__ import print_function

from unipath import Path

from ...libs import ingest_utils
from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...libs.excel_wrapper import ExcelWrapper
from . import files
from glob import glob

import re

logger = make_logger(__name__)


class StemcellsTranscriptomeMetadata(BaseMetadata):
    contextual_classes = []
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/transcriptome/']
    metadata_patterns = [r'^.*\.md5', r'^.*_metadata\.xlsx']
    organization = 'bpa-stemcells'
    auth = ('stemcell', 'stemcell')
    ckan_data_type = 'stemcells-transcriptomics'

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata

    @classmethod
    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", re.compile(r'^.*sample unique id$'), ingest_utils.extract_bpa_id),
            ("sample_extaction_id", "Sample extraction ID", None),
            ("insert_size_range", "Insert size range", None),
            ("library_construction_protocol", "Library construction protocol", None),
            ("sequencer", "Sequencer", None),
            ("analysis_software_version", "CASAVA version", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        rows = list(wrapper.get_all())
        return rows

    def get_packages(self):
        logger.info("Ingesting Stemcells Transcriptomics metadata from {0}".format(self.path))
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and BPA_ID is the primary key
        all_rows = set()
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Stemcells Transcriptomics metadata file {0}".format(fname))
            all_rows.update(StemcellsTranscriptomeMetadata.parse_spreadsheet(fname))
        for row in sorted(all_rows):
            bpa_id = row.bpa_id
            if bpa_id is None:
                continue
            obj = {}
            name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
            obj.update({
                'name': name,
                'id': name,
                'bpa_id': bpa_id,
                'notes': 'Stemcell Transcriptomics %s' % (bpa_id),
                'title': 'Stemcell Transcriptomics %s' % (bpa_id),
                'insert_size_range': row.insert_size_range,
                'library_construction_protocol': row.library_construction_protocol,
                'sequencer': row.sequencer,
                'analysis_software_version': row.analysis_software_version,
                'type': self.ckan_data_type,
                'private': True,
            })
            # for contextual_source in self.contextual_metadata:
            #     obj.update(contextual_source.get(bpa_id, track_meta))
            tag_names = ['transcriptome']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_resources(self):
        return []
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(md5_file):
                resource = dict((t, file_info.get(t)) for t in ('index', 'lane', 'vendor', 'read', 'flow_cell_id', 'library', 'extraction', 'runsamplenum'))
                resource['seq_size'] = file_info.get('size')
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                legacy_url = bpa_mirror_url('bpa/sepsis/genomics/miseq/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources
