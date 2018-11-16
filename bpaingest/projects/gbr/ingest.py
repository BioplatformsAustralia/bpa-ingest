import os
import re
from . import files

from ...libs.excel_wrapper import make_field_definition as fld
from unipath import Path
from glob import glob
from ...util import make_logger, sample_id_to_ckan_name
from ...libs import ingest_utils
from urllib.parse import urljoin
from ...abstract import BaseMetadata

logger = make_logger(__name__)


run_cleanup_re = re.compile(r'^m?(\d+_\d+)')
cell_cleanup_re = re.compile(r'^(c\d+)')


def clean_run_number(s):
    # m160916_155311_42272 -> two parts; strip any leading letter
    return run_cleanup_re.match(s).groups()[0]


def clean_flow_cell_id(s):
    # strip any _s1 from the end; any trailing underscores.
    # e.g. c100899092550000001823200804231626_s1
    return cell_cleanup_re.match(s).groups()[0]


def make_pacbio_linkage(flow_cell_id, run_number):
    run_number = clean_run_number(run_number)
    flow_cell_id = clean_flow_cell_id(flow_cell_id)
    return flow_cell_id + '_' + run_number


class GbrPacbioMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/gbr/raw/pacbio/']
    metadata_url_components = ('facility_code', 'ticket')
    organization = 'bpa-great-barrier-reef'
    ckan_data_type = 'great-barrier-reef-pacbio'
    omics = 'genomics'
    technology = 'pacbio'
    auth = ("bpa", "gbr")
    resource_linkage = ('ticket', 'sample_id', 'pacbio_linkage')
    spreadsheet = {
        'fields': [
            fld('sample_id', 'Sample unique ID', coerce=ingest_utils.extract_ands_id),
            fld('sequencing_facility', 'Sequencing facility'),
            fld('index', 'index'),
            fld('pacbio', 'Library'),
            fld('library_code', 'Library code'),
            fld('library_construction_avg_insert_size', 'Library Construction - average insert size'),
            fld('insert_size_range', 'Insert size range'),
            fld('library_construction_protocol', 'Library construction protocol'),
            fld('sequencer', 'Sequencer'),
            fld('run_number', 'Run number'),
            fld('flow_cell_id', 'Run #:Flow Cell ID'),
            fld('lane_number', 'Lane number'),
            fld('casava_version', 'CASAVA version'),
        ],
        'options': {
            'header_length': 3,
            'column_name_row_index': 1,
        }
    }
    md5 = {
        'match': [files.pacbio_filename_re, files.pacbio_filename2_re],
        'skip': None,
    }

    def __init__(self, metadata_path, metadata_info=None):
        super(GbrPacbioMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Pacbio metadata file {0}".format(fname))
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                xlsx_info = self.metadata_info[os.path.basename(fname)]
                sample_id = row.sample_id
                if sample_id is None:
                    continue

                pacbio_linkage = make_pacbio_linkage(row.flow_cell_id, row.run_number)
                name = sample_id_to_ckan_name(ingest_utils.short_ands_id(sample_id), self.ckan_data_type, pacbio_linkage)

                obj = {
                    'name': name,
                    'id': name,
                    'title': 'Pacbio {} {}'.format(sample_id, row.flow_cell_id),
                    'notes': 'Pacbio {} {}'.format(sample_id, row.flow_cell_id),
                    'tags': [{'name': 'Pacbio'}],
                    'type': GbrPacbioMetadata.ckan_data_type,
                    'private': True,
                    'sample_id': sample_id,
                    'sequencing_facility': row.sequencing_facility,
                    'ticket': xlsx_info['ticket'],
                    'run_number': row.run_number,
                    'flow_cell_id': row.flow_cell_id,
                    'library_code': row.library_code,
                    'library_construction_avg_insert_size': row.library_construction_avg_insert_size,
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'lane_number': row.lane_number,
                    'casava_version': row.casava_version,
                    'pacbio_linkage': pacbio_linkage,
                }
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                sample_id = ingest_utils.extract_ands_id(file_info['sample_id'])
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                pacbio_linkage = make_pacbio_linkage(file_info['flow_cell_id'], file_info['run_number'])
                resources.append(((xlsx_info['ticket'], sample_id, pacbio_linkage), legacy_url, resource))
        return resources


class GbrAmpliconsMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/gbr/raw/amplicons/']
    metadata_url_components = ('amplicon_', 'facility_code', 'ticket')
    organization = 'bpa-great-barrier-reef'
    ckan_data_type = 'great-barrier-reef-amplicon'
    omics = 'genomics'
    technology = 'amplicons'
    auth = ("bpa", "gbr")
    resource_linkage = ('sample_id', 'amplicon', 'index')
    extract_index_re = re.compile('^.*_([GATC]{8}_[GATC]{8})$')
    spreadsheet = {
        'fields': [
            fld('sample_id', 'Sample unique ID', coerce=ingest_utils.extract_ands_id),
            fld('sample_extraction_id', 'Sample extraction ID', coerce=ingest_utils.fix_sample_extraction_id),
            fld('sequencing_facility', 'Sequencing facility'),
            fld('target_range', 'Target Range'),
            fld('amplicon', 'Target', coerce=lambda s: s.upper().strip().lower()),
            fld('i7_index', 'I7_Index_ID'),
            fld('i5_index', 'I5_Index_ID'),
            fld('index1', 'index'),
            fld('index2', 'index2'),
            fld('pcr_1_to_10', '1:10 PCR, P=pass, F=fail', coerce=ingest_utils.fix_pcr),
            fld('pcr_1_to_100', '1:100 PCR, P=pass, F=fail', coerce=ingest_utils.fix_pcr),
            fld('pcr_neat', 'neat PCR, P=pass, F=fail', coerce=ingest_utils.fix_pcr),
            fld('dilution', 'Dilution used', coerce=ingest_utils.fix_date_interval),
            fld('sequencing_run_number', 'Sequencing run number'),
            fld('flow_cell_id', 'Flowcell'),
            fld('reads', '# of reads', coerce=ingest_utils.get_int),
            fld('name', 'Sample name on sample sheet'),
            fld('analysis_software_version', 'AnalysisSoftwareVersion'),
            fld('comments', 'Comments',),
        ],
        'options': {
            'header_length': 3,
            'column_name_row_index': 1,
        }
    }
    md5 = {
        'match': [files.amplicon_filename_re],
        'skip': None,
    }

    def __init__(self, metadata_path, metadata_info=None):
        super(GbrAmpliconsMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Stemcells Transcriptomics metadata file {0}".format(fname))
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                sample_id = row.sample_id
                if sample_id is None:
                    continue
                index = self.extract_index_re.match(row.name).groups()[0].upper()
                amplicon = row.amplicon.upper()
                name = sample_id_to_ckan_name(sample_id, self.ckan_data_type + '-' + amplicon, index)
                obj = {
                    'name': name,
                    'id': name,
                    'sample_id': sample_id,
                    'title': 'Amplicon {} {}'.format(sample_id, index),
                    'notes': 'Amplicon {} {}'.format(sample_id, index),
                    'tags': [{'name': 'Amplicon'}],
                    'type': GbrAmpliconsMetadata.ckan_data_type,
                    'private': True,
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
                }
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                sample_id = ingest_utils.extract_ands_id(file_info['sample_id'])
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((sample_id, file_info['amplicon'], file_info['index']), legacy_url, resource))
        return resources
