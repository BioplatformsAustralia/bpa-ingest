from __future__ import print_function

import os
import re
from . import files

from ...libs.excel_wrapper import ExcelWrapper
from unipath import Path
from glob import glob
from ...util import make_logger, bpa_id_to_ckan_name
from ...libs import ingest_utils
from urlparse import urljoin
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class GbrAmpliconsMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/gbr/raw/amplicons/']
    metadata_url_components = ('amplicon_', 'facility_code', 'ticket')
    organization = 'bpa-great-barrier-reef'
    ckan_data_type = 'great-barrier-reef-amplicon'
    auth = ("bpa", "gbr")
    resource_linkage = ('bpa_id', 'amplicon', 'index')
    extract_index_re = re.compile('^.*_([GATC]{8}_[GATC]{8})$')

    def __init__(self, metadata_path, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info

    @classmethod
    def parse_spreadsheet(cls, file_name, additional_context):
        field_spec = [
            ('bpa_id', 'Sample unique ID', ingest_utils.extract_bpa_id),
            ('sample_extraction_id', 'Sample extraction ID', ingest_utils.fix_sample_extraction_id),
            ('sequencing_facility', 'Sequencing facility'),
            ('target_range', 'Target Range'),
            ('amplicon', 'Target', lambda s: s.upper().strip().lower()),
            ('i7_index', 'I7_Index_ID'),
            ('i5_index', 'I5_Index_ID'),
            ('index1', 'index'),
            ('index2', 'index2'),
            ('pcr_1_to_10', '1:10 PCR, P=pass, F=fail', ingest_utils.fix_pcr),
            ('pcr_1_to_100', '1:100 PCR, P=pass, F=fail', ingest_utils.fix_pcr),
            ('pcr_neat', 'neat PCR, P=pass, F=fail', ingest_utils.fix_pcr),
            ('dilution', 'Dilution used', ingest_utils.fix_date_interval),
            ('sequencing_run_number', 'Sequencing run number'),
            ('flow_cell_id', 'Flowcell'),
            ('reads', '# of reads', ingest_utils.get_int),
            ('name', 'Sample name on sample sheet'),
            ('analysis_software_version', 'AnalysisSoftwareVersion'),
            ('comments', 'Comments',),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            file_name,
            sheet_name=None,
            header_length=3,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=additional_context)

        return wrapper.get_all()

    def get_packages(self):
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Stemcells Transcriptomics metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            for row in self.parse_spreadsheet(fname, xlsx_info):
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                index = self.extract_index_re.match(row.name).groups()[0].upper()
                amplicon = row.amplicon.upper()
                name = bpa_id_to_ckan_name(bpa_id, self.ckan_data_type + '-' + amplicon, index)
                obj = {
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'title': 'Amplicon {} {}'.format(bpa_id, index),
                    'notes': 'Amplicon {} {}'.format(bpa_id, index),
                    'tags': [{'name': 'Amplicon'}],
                    'type': GbrAmpliconsMetadata.ckan_data_type,
                    'private': True,
                    'bpa_id': row.bpa_id,
                    'sample_extraction_id': row.sample_extraction_id,
                    'sequencing_facility': row.sequencing_facility,
                    'amplicon': amplicon,
                    'i7_index': row.i7_index,
                    'i5_index': row.i5_index,
                    'index': index,
                    'index1': row.index1,
                    'index2': row.index2,
                    'pcr_1_to_10': row.pcr_1_to_10,
                    'pcr_1_to_100': row.pcr_1_to_100,
                    'pcr_neat': row.pcr_neat,
                    'dilution': row.dilution,
                    'sequencing_run_number': row.sequencing_run_number,
                    'flow_cell_id': row.flow_cell_id,
                    'reads': row.reads,
                    'ticket': row.ticket,
                    'facility_code': row.facility_code,
                    'analysis_software_version': row.analysis_software_version,
                    'notes': row.comments,
                }
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.amplicon_filename_re):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                bpa_id = ingest_utils.extract_bpa_id(file_info['bpa_id'])
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_id, file_info['amplicon'], file_info['index']), legacy_url, resource))
        return resources
