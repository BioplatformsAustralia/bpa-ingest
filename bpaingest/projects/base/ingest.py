from __future__ import print_function

from unipath import Path

from ...abstract import BaseMetadata

from ...util import make_logger, bpa_id_to_ckan_name
from urlparse import urljoin

from glob import glob

from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper
from . import files
from .tracking import BASETrackMetadata
from .contextual import BASESampleContextual

import json
import os
import re

logger = make_logger(__name__)


def add_spatial_extra(package):
    "add a ckanext-spatial extra to the package"
    lat = ingest_utils.get_clean_number(package.get('latitude'))
    lng = ingest_utils.get_clean_number(package.get('longitude'))
    if not lat or not lng:
        return
    geo = {
        "type": "Point",
        "coordinates": [lng, lat]
    }
    package['spatial'] = json.dumps(geo)


class BASEAmpliconsMetadata(BaseMetadata):
    auth = ('base', 'base')
    organization = 'bpa-base'
    ckan_data_type = 'base-genomics-amplicon'
    contextual_classes = [BASESampleContextual]
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/base/raw/amplicons/',
    ]
    metadata_url_components = ('amplicon', 'facility_code', 'ticket')
    resource_linkage = ('sample_extraction_id', 'amplicon', 'flow_id')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = BASETrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            ("bpa_id", "Soil sample unique ID", ingest_utils.extract_bpa_id),
            ("sample_extraction_id", "Sample extraction ID", ingest_utils.fix_sample_extraction_id),
            ("sequencing_facility", "Sequencing facility", None),
            ("target", "Target", lambda s: s.upper().strip()),
            ("index", "Index", lambda s: s[:12]),
            ("index1", "Index 1", lambda s: s[:12]),
            ("index2", "Index2", lambda s: s[:12]),
            ("pcr_1_to_10", "1:10 PCR, P=pass, F=fail", ingest_utils.fix_pcr),
            ("pcr_1_to_100", "1:100 PCR, P=pass, F=fail", ingest_utils.fix_pcr),
            ("pcr_neat", "neat PCR, P=pass, F=fail", ingest_utils.fix_pcr),
            ("dilution", "Dilution used", ingest_utils.fix_date_interval),
            ("sequencing_run_number", "Sequencing run number", None),
            ("flow_cell_id", "Flowcell", None),
            ("reads", ("# of RAW reads", "# of reads"), ingest_utils.get_int),
            ("sample_name", "Sample name on sample sheet", None),
            ("analysis_software_version", "AnalysisSoftwareVersion", None),
            ("comments", "Comments", None),
        ]
        try:
            wrapper = ExcelWrapper(
                field_spec,
                fname,
                sheet_name=None,
                header_length=2,
                column_name_row_index=1,
                formatting_info=True,
                additional_context=metadata_info[os.path.basename(fname)])
            rows = list(wrapper.get_all())
            return rows
        except:
            logger.error("Cannot parse: `%s'" % (fname))
            return []

    def get_packages(self):
        xlsx_re = re.compile(r'^.*_(\w+)_metadata.*\.xlsx$')

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        logger.info("Ingesting BASE Amplicon metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing BASE Amplicon metadata file {0}".format(os.path.basename(fname)))
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)
                flow_id = get_flow_id(fname)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                sample_extraction_id = ingest_utils.make_sample_extraction_id(row.sample_extraction_id, bpa_id)
                obj = {}
                amplicon = row.amplicon.upper()
                name = bpa_id_to_ckan_name(sample_extraction_id, self.ckan_data_type + '-' + amplicon, flow_id)
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'flow_id': flow_id,
                    'sample_extraction_id': sample_extraction_id,
                    'target': row.target,
                    'index': row.index,
                    'index1': row.index1,
                    'index2': row.index2,
                    'pcr_1_to_10': row.pcr_1_to_10,
                    'pcr_1_to_100': row.pcr_1_to_100,
                    'pcr_neat': row.pcr_neat,
                    'dilution': row.dilution,
                    'sequencing_run_number': row.sequencing_run_number,
                    'flow_cell_id': row.flow_cell_id,
                    'reads': row.reads,
                    'sample_name': row.sample_name,
                    'analysis_software_version': row.analysis_software_version,
                    'amplicon': amplicon,
                    'notes': 'BASE Amplicons %s %s' % (amplicon, sample_extraction_id),
                    'title': 'BASE Amplicons %s %s' % (amplicon, sample_extraction_id),
                    'date_of_transfer': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'data_type': track_get('data_type'),
                    'description': track_get('description'),
                    'folder_name': track_get('folder_name'),
                    'sample_submission_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'contextual_data_submission_date': None,
                    'data_generated': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                    'dataset_url': track_get('download'),
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'type': self.ckan_data_type,
                    'comments': row.comments,
                    'private': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id))
                add_spatial_extra(obj)
                tag_names = ['amplicons', amplicon]
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting BASE Amplicon md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.amplicon_regexps):
                if filename.endswith('_metadata.xlsx') or filename.find('SampleSheet') != -1:
                    continue
                if file_info is None:
                    if not files.amplicon_control_filename_re.match(filename):
                        logger.debug("unable to parse filename: `%s'" % (filename))
                    continue
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                sample_extraction_id = bpa_id.split('.')[-1] + '_' + file_info.get('extraction')
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((sample_extraction_id, resource['amplicon'], resource['flow_id']), legacy_url, resource))
        return resources


class BASEMetagenomicsMetadata(BaseMetadata):
    auth = ('base', 'base')
    organization = 'bpa-base'
    ckan_data_type = 'base-metagenomics'
    contextual_classes = [BASESampleContextual]
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/base/raw/metagenomics/',
    ]
    metadata_url_components = ('facility_code', 'ticket')
    resource_linkage = ('sample_extraction_id', 'flow_id')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = BASETrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            ('bpa_id', 'Soil sample unique ID', ingest_utils.extract_bpa_id),
            ('sample_extraction_id', 'Sample extraction ID', ingest_utils.fix_sample_extraction_id),
            ('insert_size_range', 'Insert size range', None),
            ('library_construction_protocol', 'Library construction protocol', None),
            ('sequencer', 'Sequencer', None),
            ('casava_version', 'CASAVA version', None)
        ]
        try:
            wrapper = ExcelWrapper(
                field_spec,
                fname,
                sheet_name=None,
                header_length=2,
                column_name_row_index=1,
                formatting_info=True,
                additional_context=metadata_info[os.path.basename(fname)])
            rows = list(wrapper.get_all())
            return rows
        except:
            logger.error("Cannot parse: `%s'" % (fname))
            return []

    def get_packages(self):
        xlsx_re = re.compile(r'^.*_(\w+)_metadata.*\.xlsx$')

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        logger.info("Ingesting BASE Metagenomics metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing BASE Metagenomics metadata file {0}".format(os.path.basename(fname)))
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)
                flow_id = get_flow_id(fname)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                sample_extraction_id = ingest_utils.make_sample_extraction_id(row.sample_extraction_id, bpa_id)
                obj = {}
                name = bpa_id_to_ckan_name(sample_extraction_id, self.ckan_data_type, flow_id)
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'flow_id': flow_id,
                    'sample_extraction_id': sample_extraction_id,
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'analysis_software_version': row.casava_version,
                    'notes': 'BASE Metagenomics %s' % (sample_extraction_id),
                    'title': 'BASE Metagenomics %s' % (sample_extraction_id),
                    'date_of_transfer': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'data_type': track_get('data_type'),
                    'description': track_get('description'),
                    'folder_name': track_get('folder_name'),
                    'sample_submission_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'contextual_data_submission_date': None,
                    'data_generated': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                    'dataset_url': track_get('download'),
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'type': self.ckan_data_type,
                    'private': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id))
                add_spatial_extra(obj)
                tag_names = ['metagenomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting BASE Metagenomics md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.metagenomics_regexps):
                if filename.endswith('_metadata.xlsx') or filename.find('SampleSheet') != -1:
                    continue
                if file_info is None:
                    logger.debug("unable to parse filename: `%s'" % (filename))
                    continue
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                sample_extraction_id = bpa_id.split('.')[-1] + '_' + file_info.get('extraction')
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((sample_extraction_id, resource['flow_id']), legacy_url, resource))
        return resources
