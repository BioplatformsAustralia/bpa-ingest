from __future__ import print_function

from unipath import Path

from ...abstract import BaseMetadata

from ...util import make_logger, bpa_id_to_ckan_name
from urlparse import urljoin

from glob import glob

from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper
from . import files
from .tracking import OMGTrackMetadata
from .contextual import OMGSampleContextual

import os
import re

logger = make_logger(__name__)


class OMG10XRawIlluminaMetadata(BaseMetadata):
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-10x-raw-illumina'
    contextual_classes = [OMGSampleContextual]
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_raw/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_id', 'flow_id')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(track_csv_path)
        # each row in the spreadsheet maps through to a single tar file
        self.file_package = {}

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            ("bpa_id", "BPA ID", ingest_utils.extract_bpa_id),
            ("file", "file", None),
            ("library_preparation", "library prep", None),
            ("analysis_software_version", "softwareverion", None),
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

        logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing OMG metadata file {0}".format(os.path.basename(fname)))
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
                obj = {}
                name = bpa_id_to_ckan_name(bpa_id, self.ckan_data_type, flow_id)
                self.file_package[row.file] = (bpa_id, flow_id)
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'flow_id': flow_id,
                    'library_preparation': row.library_preparation,
                    'analysis_software_version': row.analysis_software_version,
                    'notes': 'OMG 10x Illumina Raw %s %s' % (bpa_id, flow_id),
                    'title': 'OMG 10x Illumina Raw %s %s' % (bpa_id, flow_id),
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
                    'type': self.ckan_data_type,
                    'private': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id))
                tag_names = ['10x-raw']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting OMG md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, [files.tenxtar_filename_re]):
                # FIXME: we should upload these somewhere centrally
                if filename.endswith('_metadata.xlsx') or filename.find('SampleSheet') != -1:
                    continue
                if file_info is None:
                    logger.debug("unable to parse filename: `%s'" % (filename))
                    continue
                bpa_id, flow_id = self.file_package[filename]
                resource = file_info.copy()
                # waiting on filename convention from AGRF
                del resource['basename']
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_id, flow_id), legacy_url, resource))
        return resources


class OMG10XProcessedIlluminaMetadata(BaseMetadata):
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-10x-processed-illumina'
    contextual_classes = [OMGSampleContextual]
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_processed/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_id', 'flow_id')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata(track_csv_path)
        # each row in the spreadsheet maps through to a single tar file
        self.file_package = {}

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            ("bpa_id", "BPA ID", ingest_utils.extract_bpa_id),
            ("file", "file", None),
            ("library_preparation", "library prep", None),
            ("analysis_software_version", "softwareverion", None),
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

        logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing OMG metadata file {0}".format(os.path.basename(fname)))
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
                obj = {}
                name = bpa_id_to_ckan_name(bpa_id, self.ckan_data_type, flow_id)
                self.file_package[row.file] = bpa_id, flow_id
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'flow_id': flow_id,
                    'library_preparation': row.library_preparation,
                    'analysis_software_version': row.analysis_software_version,
                    'notes': 'OMG 10x Illumina Processed %s %s' % (bpa_id, flow_id),
                    'title': 'OMG 10x Illumina Processed %s %s' % (bpa_id, flow_id),
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
                    'type': self.ckan_data_type,
                    'private': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id))
                tag_names = ['10x-processed']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting OMG md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, [files.tenxtar_filename_re]):
                # FIXME: we should upload these somewhere centrally
                if filename.endswith('_metadata.xlsx') or filename.find('SampleSheet') != -1:
                    continue
                if file_info is None:
                    logger.debug("unable to parse filename: `%s'" % (filename))
                    continue
                bpa_id, flow_id = self.file_package[filename]
                resource = file_info.copy()
                # waiting on filename convention from AGRF
                del resource['basename']
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_id, flow_id), legacy_url, resource))
        return resources