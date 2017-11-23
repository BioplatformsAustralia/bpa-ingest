

from unipath import Path
from collections import defaultdict

from ...abstract import BaseMetadata

from ...util import make_logger, bpa_id_to_ckan_name, common_values
from urllib.parse import urljoin

from glob import glob

from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from . import files
from .tracking import OMGTrackMetadata
from .contextual import (OMGSampleContextual, OMGLibraryContextual)

import os
import re

logger = make_logger(__name__)
common_context = [OMGSampleContextual, OMGLibraryContextual]


class OMG10XRawIlluminaMetadata(BaseMetadata):
    """
    early run data, produced at AGRF.

    This data is unusual: it may contain more than one sample/library ID
    in a single tar file. It's been confirmed by AGRF that the data cannot
    be split by sample if this has happened.

    We use flow_id as the single key for resource linkage, and we then
    use the spreadsheet to determine the [library, sample, dataset] IDs
    for each tar file and present the metadata for each to the user.
    """

    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-10x-raw-illumina'
    technology = '10x-raw-agrf'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_raw_agrf/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('archive_name',)
    spreadsheet = {
        'fields': [
            fld('bpa_dataset_id', 'bpa_dataset_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_library_id', 'bpa_library_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_sample_id', 'bpa_sample_id', coerce=ingest_utils.extract_bpa_id),
            fld('facility_sample_id', 'facility_sample_id'),
            fld('library_type', 'library_type'),
            fld('library_prep_date', 'library_prep_date'),
            fld('library_prepared_by', 'library_prepared_by'),
            fld('library_prep_method', 'library_prep_method'),
            fld('experimental_design', 'experimental_design'),
            fld('omg_project', 'omg_project'),
            fld('data_custodian', 'data_custodian'),
            fld('dna_treatment', 'dna_treatment'),
            fld('library_index_id', 'library_index_id'),
            fld('library_index_sequence', 'library_index_sequence'),
            fld('library_oligo_sequence', 'library_oligo_sequence'),
            fld('library_pcr_reps', 'library_pcr_reps'),
            fld('library_pcr_cycles', 'library_pcr_cycles'),
            fld('library_ng_ul', 'library_ng_ul'),
            fld('library_comments', 'library_comments'),
            fld('library_location', 'library_location'),
            fld('library_status', 'library_status'),
            fld('sequencing_facility', 'sequencing_facility'),
            fld('n_libraries_pooled', 'n_libraries_pooled'),
            fld('bpa_work_order', 'bpa_work_order', coerce=ingest_utils.get_int),
            fld('sequencing_platform', 'sequencing_platform'),
            fld('sequence_length', 'sequence_length'),
            fld('flowcell_id', 'flowcell_id'),
            fld('software_version', 'software_version'),
            fld('file', 'file'),
        ],
        'options': {
            'header_length': 1,
            'column_name_row_index': 0,
        }
    }
    md5 = {
        'match': [
            files.tenxtar_filename_re
        ],
        'skip': [
            re.compile(r'^.*_metadata\.xlsx$'),
            re.compile(r'^.*SampleSheet.*'),
            re.compile(r'^.*TestFiles\.exe.*'),
        ]
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMG10XRawIlluminaMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()
        # each row in the spreadsheet maps through to a single tar file
        self.file_package = {}

    def _get_packages(self):
        xlsx_re = re.compile(r'^.*_(\w+)_metadata.*\.xlsx$')

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        logger.info("Ingesting OMG metadata from {0}".format(self.path))

        def make_row_metadata(row):
            row_obj = {}
            context = {}
            for contextual_source in self.contextual_metadata:
                context.update(contextual_source.get(row.bpa_sample_id, row.bpa_library_id))
            row_obj.update(row._asdict())
            row_obj.update(context)
            return row_obj

        # glomp together the spreadsheet rows by filename
        fname_rows = defaultdict(list)

        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing OMG metadata file {0}".format(os.path.basename(fname)))
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                fname_rows[(get_flow_id(fname), row.file)].append(row)

        packages = []
        for (flow_id, fname), rows in fname_rows.items():
            name = bpa_id_to_ckan_name(fname, self.ckan_data_type, flow_id)
            assert(fname not in self.file_package)
            self.file_package[fname] = fname
            row_metadata = [make_row_metadata(row) for row in rows]

            bpa_sample_ids = ', '.join([t.bpa_sample_id for t in rows])
            bpa_dataset_ids = ', '.join([t.bpa_dataset_id for t in rows])
            bpa_library_ids = ', '.join([t.bpa_library_id for t in rows])

            obj = {
                'name': name,
                'id': name,
                'flow_id': flow_id,
                'bpa_sample_ids': bpa_sample_ids,
                'bpa_library_ids': bpa_library_ids,
                'bpa_dataset_ids': bpa_dataset_ids,
                'title': 'OMG 10x Illumina Raw %s %s' % (bpa_sample_ids, flow_id),
                'archive_name': fname,
                'type': self.ckan_data_type,
                'private': True,
            }
            # there must be only one ticket
            assert(len(set(t.ticket for t in rows)) == 1)

            ticket = rows[0].ticket
            track_meta = self.track_meta.get(ticket)

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            notes = '\n'.join('%s. %s.' % (t.get('common_name', ''), t.get('institution_name', '')) for t in row_metadata)

            obj.update({
                'ticket': ticket,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                'data_type': track_get('data_type'),
                'description': track_get('description'),
                'folder_name': track_get('folder_name'),
                'sample_submission_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                'dataset_url': track_get('download'),
                'notes': notes,
            })
            ingest_utils.add_spatial_extra(obj)
            obj.update(common_values([make_row_metadata(row) for row in rows]))

            tag_names = ['10x-raw']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)

        return packages

    def _get_resources(self):
        logger.info("Ingesting OMG md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            for filename, md5, file_info in self.parse_md5file(md5_file):
                archive_name = self.file_package[filename]
                resource = file_info.copy()
                # waiting on filename convention from AGRF
                del resource['basename']
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((archive_name,), legacy_url, resource))
        return resources


class OMG10XRawMetadata(BaseMetadata):
    """
    this data conforms to the BPA 10X raw workflow. future data
    will use this ingest class.
    """
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-10x-raw'
    technology = '10xraw'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_raw/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_sample_id', 'flow_id')
    spreadsheet = {
        'fields': [
            fld('bpa_dataset_id', 'bpa_dataset_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_library_id', 'bpa_library_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_sample_id', 'bpa_sample_id', coerce=ingest_utils.extract_bpa_id),
            fld('facility_sample_id', 'facility_sample_id'),
            fld('library_type', 'library_type'),
            fld('library_prep_date', 'library_prep_date'),
            fld('library_prepared_by', 'library_prepared_by'),
            fld('library_prep_method', 'library_prep_method'),
            fld('experimental_design', 'experimental_design'),
            fld('omg_project', 'omg_project'),
            fld('data_custodian', 'data_custodian'),
            fld('dna_treatment', 'dna_treatment'),
            fld('library_index_id', 'library_index_id'),
            fld('library_index_sequence', 'library_index_sequence'),
            fld('library_oligo_sequence', 'library_oligo_sequence'),
            fld('library_pcr_reps', 'library_pcr_reps'),
            fld('library_pcr_cycles', 'library_pcr_cycles'),
            fld('library_ng_ul', 'library_ng_ul'),
            fld('library_comments', 'library_comments'),
            fld('library_location', 'library_location'),
            fld('library_status', 'library_status'),
            fld('sequencing_facility', 'sequencing_facility'),
            fld('n_libraries_pooled', 'n_libraries_pooled'),
            fld('bpa_work_order', 'bpa_work_order', coerce=ingest_utils.get_int),
            fld('sequencing_platform', 'sequencing_platform'),
            fld('sequence_length', 'sequence_length'),
            fld('flowcell_id', 'flowcell_id'),
            fld('software_version', 'software_version'),
            fld('file', 'file'),
        ],
        'options': {
            'header_length': 1,
            'column_name_row_index': 0,
        }
    }
    md5 = {
        'match': [
            files.tenxfastq_filename_re
        ],
        'skip': [
            re.compile(r'^.*_metadata\.xlsx$'),
            re.compile(r'^.*SampleSheet.*'),
            re.compile(r'^.*TestFiles\.exe.*'),
        ]
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMG10XRawMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()
        self.flow_lookup = {}

    def _get_packages(self):
        logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing OMG metadata file {0}".format(os.path.basename(fname)))

            # for this tech, each spreadsheet will only have a single BPA ID and flow cell
            # we grab the common values in the spreadsheet, then apply the flow cell ID
            # from the filename
            obj = common_values([t._asdict() for t in self.parse_spreadsheet(fname, self.metadata_info)])
            file_info = files.tenx_raw_xlsx_filename_re.match(os.path.basename(fname)).groupdict()
            obj['flow_id'] = file_info['flow_id']

            bpa_sample_id = obj['bpa_sample_id']
            bpa_library_id = obj['bpa_library_id']
            flow_id = obj['flow_id']
            self.flow_lookup[obj['ticket']] = flow_id

            name = bpa_id_to_ckan_name(bpa_sample_id, self.ckan_data_type, flow_id)
            context = {}
            for contextual_source in self.contextual_metadata:
                context.update(contextual_source.get(bpa_sample_id, bpa_library_id))

            track_meta = self.track_meta.get(obj['ticket'])

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            obj.update({
                'name': name,
                'id': name,
                'bpa_sample_id': bpa_sample_id,
                'title': 'OMG 10x Raw %s %s' % (bpa_sample_id, flow_id),
                'notes': '%s. %s.' % (context.get('common_name', ''), context.get('institution_name', '')),
                'date_of_transfer': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                'data_type': track_get('data_type'),
                'description': track_get('description'),
                'folder_name': track_get('folder_name'),
                'sample_submission_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                'dataset_url': track_get('download'),
                'type': self.ckan_data_type,
                'private': True,
            })
            obj.update(context)
            ingest_utils.add_spatial_extra(obj)
            tag_names = ['10x-raw']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting OMG md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                ticket = xlsx_info['ticket']
                flow_id = self.flow_lookup[ticket]
                bpa_sample_id = ingest_utils.extract_bpa_id(file_info['bpa_sample_id'])

                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_sample_id, flow_id), legacy_url, resource))
        return resources


class OMG10XProcessedIlluminaMetadata(BaseMetadata):
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-10x-processed-illumina'
    technology = '10xprocessed'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$', r'^.*_processed.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_processed/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_sample_id', 'flow_id')
    spreadsheet = {
        'fields': [
            fld('bpa_dataset_id', 'bpa_dataset_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_library_id', 'bpa_library_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_sample_id', 'bpa_sample_id', coerce=ingest_utils.extract_bpa_id),
            fld('facility_sample_id', 'facility_sample_id'),
            fld('library_type', 'library_type'),
            fld('library_prep_date', 'library_prep_date'),
            fld('library_prepared_by', 'library_prepared_by'),
            fld('library_prep_method', 'library_prep_method'),
            fld('experimental_design', 'experimental_design'),
            fld('omg_project', 'omg_project'),
            fld('data_custodian', 'data_custodian'),
            fld('dna_treatment', 'dna_treatment'),
            fld('library_index_id', 'library_index_id'),
            fld('library_index_sequence', 'library_index_sequence'),
            fld('library_oligo_sequence', 'library_oligo_sequence'),
            fld('library_pcr_reps', 'library_pcr_reps'),
            fld('library_pcr_cycles', 'library_pcr_cycles'),
            fld('library_ng_ul', 'library_ng_ul'),
            fld('library_comments', 'library_comments'),
            fld('library_location', 'library_location'),
            fld('library_status', 'library_status'),
            fld('sequencing_facility', 'sequencing_facility'),
            fld('n_libraries_pooled', 'n_libraries_pooled'),
            fld('bpa_work_order', 'bpa_work_order', coerce=ingest_utils.get_int),
            fld('sequencing_platform', 'sequencing_platform'),
            fld('sequence_length', 'sequence_length'),
            fld('flowcell_id', 'flowcell_id'),
            fld('software_version', 'software_version'),
            fld('file', 'file'),
        ],
        'options': {
            'header_length': 1,
            'column_name_row_index': 0,
        }
    }
    md5 = {
        'match': [
            files.tenxtar_filename_re
        ],
        'skip': [
            re.compile(r'^.*_processed\.xlsx$'),
            re.compile(r'^.*SampleSheet.*'),
            re.compile(r'^.*TestFiles\.exe.*'),
        ]
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMG10XProcessedIlluminaMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()
        # each row in the spreadsheet maps through to a single tar file
        self.file_package = {}

    def _get_packages(self):
        xlsx_re = re.compile(r'^.*_(\w+)_processed.*\.xlsx$')

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
                bpa_sample_id = row.bpa_sample_id
                bpa_library_id = row.bpa_library_id
                if bpa_sample_id is None:
                    continue
                obj = {}
                name = bpa_id_to_ckan_name(bpa_sample_id, self.ckan_data_type, flow_id)
                assert(row.file not in self.file_package)
                self.file_package[row.file] = bpa_sample_id, flow_id
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(bpa_sample_id, bpa_library_id))
                obj.update(row._asdict())
                obj.update({
                    'name': name,
                    'id': name,
                    'flow_id': flow_id,
                    'title': 'OMG 10x Illumina Processed %s %s' % (bpa_sample_id, flow_id),
                    'notes': '%s. %s.' % (context.get('common_name', ''), context.get('institution_name', '')),
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
                obj.update(context)
                ingest_utils.add_spatial_extra(obj)
                tag_names = ['10x-processed']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting OMG md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                bpa_sample_id, flow_id = self.file_package[filename]
                resource = file_info.copy()
                # waiting on filename convention from AGRF
                del resource['basename']
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_sample_id, flow_id), legacy_url, resource))
        return resources


class OMGExonCaptureMetadata(BaseMetadata):
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-exon-capture'
    technology = 'exoncapture'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/exon_capture/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_library_id', 'flowcell_id', 'library_index_sequence')
    spreadsheet = {
        'fields': [
            fld('bpa_dataset_id', 'bpa_dataset_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_library_id', 'bpa_library_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_sample_id', 'bpa_sample_id', coerce=ingest_utils.extract_bpa_id),
            fld('facility_sample_id', 'facility_sample_id'),
            fld('library_type', 'library_type'),
            fld('library_prep_date', 'library_prep_date', coerce=ingest_utils.get_date_isoformat),
            fld('library_prepared_by', 'library_prepared_by'),
            fld('library_prep_method', 'library_prep_method'),
            fld('experimental_design', 'experimental_design'),
            fld('omg_project', 'omg_project'),
            fld('data_custodian', 'data_custodian'),
            fld('dna_treatment', 'dna_treatment'),
            fld('library_index_id', 'library_index_id'),
            fld('library_index_sequence', 'library_index_sequence'),
            fld('library_oligo_sequence', 'library_oligo_sequence'),
            fld('library_pcr_reps', 'library_pcr_reps'),
            fld('library_pcr_cycles', 'library_pcr_cycles'),
            fld('library_ng_ul', 'library_ng_ul'),
            fld('library_comments', 'library_comments'),
            fld('library_location', 'library_location'),
            fld('library_status', 'library_status'),
            fld('sequencing_facility', 'sequencing_facility'),
            fld('n_libraries_pooled', 'n_libraries_pooled'),
            fld('bpa_work_order', 'bpa_work_order', coerce=ingest_utils.get_int),
            fld('sequencing_platform', 'sequencing_platform'),
            fld('sequence_length', 'sequence_length'),
            fld('flowcell_id', 'flowcell_id'),
            fld('software_version', 'software_version'),
            fld('file', 'file'),
        ],
        'options': {
            'header_length': 1,
            'column_name_row_index': 0,
        }
    }
    md5 = {
        'match': [
            files.exon_filename_re
        ],
        'skip': [
            re.compile(r'^.*_metadata\.xlsx$'),
            re.compile(r'^.*SampleSheet.*'),
            re.compile(r'^.*TestFiles\.exe.*'),
        ]
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMGExonCaptureMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()

    @classmethod
    def flow_cell_index_linkage(cls, flow_id, index):
        return flow_id + '_' + index.replace('-', '').replace('_', '')

    def _get_packages(self):
        logger.info("Ingesting OMG metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing OMG metadata file {0}".format(os.path.basename(fname)))
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)

                library_id = row.bpa_library_id
                if library_id is None:
                    continue
                linkage = self.flow_cell_index_linkage(row.flowcell_id, row.library_index_sequence)
                name = bpa_id_to_ckan_name(library_id, self.ckan_data_type, linkage)
                obj = row._asdict()
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(row.bpa_sample_id, row.bpa_library_id))
                obj.update({
                    'name': name,
                    'id': name,
                    'title': 'OMG Exon Capture Raw %s %s %s' % (library_id, row.flowcell_id, row.library_index_sequence),
                    'notes': '%s. %s.' % (context.get('common_name', ''), context.get('institution_name', '')),
                    'date_of_transfer': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'data_type': track_get('data_type'),
                    'description': track_get('description'),
                    'folder_name': track_get('folder_name'),
                    'sample_submission_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'contextual_data_submission_date': None,
                    'data_generated': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                    'dataset_url': track_get('download'),
                    'type': self.ckan_data_type,
                    'private': True,
                })
                obj.update(context)
                ingest_utils.add_spatial_extra(obj)
                tag_names = ['exon-capture', 'raw']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting OMG md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                library_id = ingest_utils.extract_bpa_id(resource['bpa_library_id'])
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((library_id, resource['flow_cell_id'], resource['index']), legacy_url, resource))
        return resources


class OMGGenomicsHiSeqMetadata(BaseMetadata):
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-genomics-hiseq'
    omics = 'genomics'
    technology = 'hiseq'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/genomics/raw/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_sample_id', 'flowcell_id')
    spreadsheet = {
        'fields': [
            fld('bpa_dataset_id', 'bpa_dataset_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_library_id', 'bpa_library_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_sample_id', 'bpa_sample_id', coerce=ingest_utils.extract_bpa_id),
            fld('facility_sample_id', 'facility_sample_id'),
            fld('library_type', 'library_type'),
            fld('library_prep_date', 'library_prep_date'),
            fld('library_prepared_by', 'library_prepared_by'),
            fld('library_prep_method', 'library_prep_method'),
            fld('experimental_design', 'experimental_design'),
            fld('omg_project', 'omg_project'),
            fld('data_custodian', 'data_custodian'),
            fld('dna_treatment', 'dna_treatment'),
            fld('library_index_id', 'library_index_id'),
            fld('library_index_sequence', 'library_index_sequence'),
            fld('library_oligo_sequence', 'library_oligo_sequence'),
            fld('library_pcr_reps', 'library_pcr_reps'),
            fld('library_pcr_cycles', 'library_pcr_cycles'),
            fld('library_ng_ul', 'library_ng_ul'),
            fld('library_comments', 'library_comments'),
            fld('library_location', 'library_location'),
            fld('library_status', 'library_status'),
            fld('sequencing_facility', 'sequencing_facility'),
            fld('n_libraries_pooled', 'n_libraries_pooled'),
            fld('bpa_work_order', 'bpa_work_order', coerce=ingest_utils.get_int),
            fld('sequencing_platform', 'sequencing_platform'),
            fld('sequence_length', 'sequence_length'),
            fld('flowcell_id', 'flowcell_id'),
            fld('software_version', 'software_version'),
            fld('file', 'file'),
        ],
        'options': {
            'header_length': 1,
            'column_name_row_index': 0,
        }
    }
    md5 = {
        'match': [
            files.hiseq_filename_re
        ],
        'skip': [
            re.compile(r'^.*_metadata\.xlsx$'),
            re.compile(r'^.*SampleSheet.*'),
            re.compile(r'^.*TestFiles\.exe.*'),
        ]
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMGGenomicsHiSeqMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()

    def _get_packages(self):
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
            flow_id = get_flow_id(fname)

            objs = defaultdict(list)
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                obj = row._asdict()
                obj.pop('file')
                objs[obj['bpa_sample_id']].append(obj)

            for bpa_sample_id, row_objs in list(objs.items()):
                obj = common_values(row_objs)
                track_meta = self.track_meta.get(obj['ticket'])

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)
                bpa_sample_id = obj['bpa_sample_id']
                bpa_library_id = obj['bpa_library_id']
                if bpa_sample_id is None:
                    continue
                name = bpa_id_to_ckan_name(bpa_sample_id, self.ckan_data_type, flow_id)
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(bpa_sample_id, bpa_library_id))
                obj.update({
                    'name': name,
                    'id': name,
                    'flow_id': flow_id,
                    'title': 'OMG Genomics HiSeq Raw %s %s' % (bpa_sample_id, flow_id),
                    'notes': '%s. %s.' % (context.get('common_name', ''), context.get('institution_name', '')),
                    'date_of_transfer': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'data_type': track_get('data_type'),
                    'description': track_get('description'),
                    'folder_name': track_get('folder_name'),
                    'sample_submission_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'contextual_data_submission_date': None,
                    'data_generated': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                    'dataset_url': track_get('download'),
                    'type': self.ckan_data_type,
                    'private': True,
                })
                obj.update(context)
                ingest_utils.add_spatial_extra(obj)
                tag_names = ['genomics-hiseq']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting OMG md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((ingest_utils.extract_bpa_id(resource['bpa_sample_id']), resource['flow_cell_id']), legacy_url, resource))
        return resources
