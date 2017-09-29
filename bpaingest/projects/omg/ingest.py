

from unipath import Path
from collections import defaultdict

from ...abstract import BaseMetadata

from ...util import make_logger, bpa_id_to_ckan_name, common_values
from urllib.parse import urljoin

from glob import glob

from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper, make_field_definition as fld
from . import files
from .tracking import OMGTrackMetadata
from .contextual import OMGSampleContextual

import os
import re

logger = make_logger(__name__)


class OMG10XRawIlluminaMetadata(BaseMetadata):
    """
    early run data, produced at AGRF. No future data will be produced in this way.
    """

    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-10x-raw-illumina'
    technology = '10x-raw-agrf'
    contextual_classes = [OMGSampleContextual]
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_raw_agrf/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_id', 'flow_id')

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMG10XRawIlluminaMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()
        # each row in the spreadsheet maps through to a single tar file
        self.file_package = {}

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            fld("bpa_id", "BPA ID", coerce=ingest_utils.extract_bpa_id),
            fld("file", "file"),
            fld("library_preparation", "library prep"),
            fld("analysis_software_version", "softwareverion"),
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
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(bpa_id))
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'flow_id': flow_id,
                    'library_preparation': row.library_preparation,
                    'analysis_software_version': row.analysis_software_version,
                    'title': 'OMG 10x Illumina Raw %s %s' % (bpa_id, flow_id),
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
                tag_names = ['10x-raw']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
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


class OMG10XRawMetadata(BaseMetadata):
    """
    this data conforms to the BPA 10X raw workflow. future data
    will use this ingest class.
    """
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-10x-raw'
    technology = '10xraw'
    contextual_classes = [OMGSampleContextual]
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_raw/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_id', 'flow_id')

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMG10XRawMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()
        self.flow_lookup = {}

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            fld('bpa_id', 'bpa id', coerce=ingest_utils.extract_bpa_id),
            fld('facility', 'facility'),
            fld('file', 'file'),
            fld('library_preparation', 'library prep'),
            fld('platform', 'platform'),
            fld('software_version', 'software version'),
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

            bpa_id = obj['bpa_id']
            flow_id = obj['flow_id']
            self.flow_lookup[obj['ticket']] = flow_id

            name = bpa_id_to_ckan_name(bpa_id, self.ckan_data_type, flow_id)
            context = {}
            for contextual_source in self.contextual_metadata:
                context.update(contextual_source.get(bpa_id))

            track_meta = self.track_meta.get(obj['ticket'])

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            obj.update({
                'name': name,
                'id': name,
                'bpa_id': bpa_id,
                'flow_id': flow_id,
                'title': 'OMG 10x Raw %s %s' % (bpa_id, flow_id),
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
            for filename, md5, file_info in files.parse_md5_file(md5_file, [files.tenxfastq_filename_re]):
                # FIXME: we should upload these somewhere centrally
                if filename.endswith('_metadata.xlsx') or filename.find('SampleSheet') != -1:
                    continue
                if file_info is None:
                    logger.debug("unable to parse filename: `%s'" % (filename))
                    continue
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                ticket = xlsx_info['ticket']
                flow_id = self.flow_lookup[ticket]
                bpa_id = ingest_utils.extract_bpa_id(file_info['bpa_id'])

                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_id, flow_id), legacy_url, resource))
        return resources


class OMG10XProcessedIlluminaMetadata(BaseMetadata):
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-10x-processed-illumina'
    technology = '10xprocessed'
    contextual_classes = [OMGSampleContextual]
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/10x_processed/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_id', 'flow_id')

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMG10XProcessedIlluminaMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()
        # each row in the spreadsheet maps through to a single tar file
        self.file_package = {}

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            fld("bpa_id", "BPA ID", coerce=ingest_utils.extract_bpa_id),
            fld("file", "file"),
            fld("library_preparation", "library prep"),
            fld("analysis_software_version", "softwareverion"),
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
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(bpa_id))
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'flow_id': flow_id,
                    'library_preparation': row.library_preparation,
                    'analysis_software_version': row.analysis_software_version,
                    'title': 'OMG 10x Illumina Processed %s %s' % (bpa_id, flow_id),
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


class OMGExonCaptureMetadata(BaseMetadata):
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-exon-capture'
    technology = 'exoncapture'
    contextual_classes = [OMGSampleContextual]
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/exon_capture/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_id', 'flowcell_id', 'index_sequence')

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMGExonCaptureMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()

    @classmethod
    def flow_cell_index_linkage(cls, flow_id, index):
        return flow_id + '_' + index.replace('-', '').replace('_', '')

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            fld("bpa_id", "bpa_id", coerce=ingest_utils.extract_bpa_id),
            fld('voucher_id', 'voucher_id'),
            fld('dna_extraction_date', 'dna_extraction_date'),
            fld('dna_extracted_by', 'dna_extracted_by'),
            fld('dna_extraction_method', 'dna_extraction_method'),
            fld('sample_origin', 'sample_origin'),
            fld('dsdna_conc_ng_ul', 'dsdna__conc_ng_ul'),
            fld('experimental_design', 'experimental_design'),
            fld('library_construction_method', 'library_construction_method'),
            fld('n_samples_pooled', 'n_samples_pooled'),
            fld('capture_type', 'capture_type'),
            fld('exon_capture_pool', 'exon_capture_pool', coerce=ingest_utils.get_int),
            fld('index_used', 'index_used', coerce=ingest_utils.get_int),
            fld('brf_sample_id', 'brf_sample_id'),
            fld('oligo_id', 'oligo_id'),
            fld('oligo_sequence', 'oligo_sequence'),
            fld('index_sequence', 'index_sequence'),
            fld('index_pcr_rep1_cycles', 'index_pcr_rep1_cycles', coerce=ingest_utils.get_int),
            fld('index_pcr_rep2_cycles', 'index_pcr_rep2_cycles', coerce=ingest_utils.get_int),
            fld('indexpcr_conc_ng_ul', 'indexpcr_conc_ng_ul'),
            fld('bpa_work_order', 'bpa_work_order', coerce=ingest_utils.get_int),
            fld('sequencing_facility', 'sequencing_facility'),
            fld('sequencing_platform', 'sequencing_platform'),
            fld('sequence_length', 'sequence_length'),
            fld('flowcell_id', 'flowcell_id'),
            fld('pre_capture_conc_ng_ul', 'pre_capture_conc_ng_ul'),
            fld('hyb_duration_hours', 'hyb_duration_hours'),
            fld('post_capture_conc_ng_ul', 'post_capture_conc_ng_ul'),
            fld('qpcr_p1_meancp_pre', 'qpcr_p1_meancp_pre'),
            fld('qpcr_p1_meancp_post', 'qpcr_p1_meancp_post'),
            fld('qpcr_p2_meancp_pre', 'qpcr_p2_meancp_pre'),
            fld('qpcr_p2_meancp_post', 'qpcr_p2_meancp_post'),
            fld('qpcr_n1_meancp_pre', 'qpcr_n1_meancp_pre'),
            fld('qpcr_n1_meancp_post', 'qpcr_n1_meancp_post'),
            fld('qpcr_n2_meancp_pre', 'qpcr_n2_meancp_pre'),
            fld('qpcr_n2_meancp_post', 'qpcr_n2_meancp_post'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=1,
            column_name_row_index=0,
            formatting_info=True,
            additional_context=metadata_info[os.path.basename(fname)])
        rows = list(wrapper.get_all())
        return rows

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
                bpa_id = ingest_utils.extract_bpa_id(row.bpa_id)
                if bpa_id is None:
                    continue
                linkage = self.flow_cell_index_linkage(row.flowcell_id, row.index_sequence)
                name = bpa_id_to_ckan_name(bpa_id, self.ckan_data_type, linkage)
                obj = row._asdict()
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(bpa_id))
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'title': 'OMG Exon Capture Raw %s %s %s' % (bpa_id, row.flowcell_id, row.index_sequence),
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
            for filename, md5, file_info in files.parse_md5_file(md5_file, [files.exon_filename_re]):
                # FIXME: we should upload these somewhere centrally
                if filename.endswith('_metadata.csv') or filename.find('SampleSheet') != -1:
                    continue
                if file_info is None:
                    logger.debug("unable to parse filename: `%s'" % (filename))
                    continue
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                bpa_id = ingest_utils.extract_bpa_id(resource['bpa_id'])
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_id, resource['flow_cell_id'], resource['index']), legacy_url, resource))
        return resources


class OMGGenomicsHiSeqMetadata(BaseMetadata):
    auth = ('omg', 'omg')
    organization = 'bpa-omg'
    ckan_data_type = 'omg-genomics-hiseq'
    omics = 'genomics'
    technology = 'hiseq'
    contextual_classes = [OMGSampleContextual]
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/omg_staging/genomics/raw/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('bpa_id', 'flow_id')

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(OMGGenomicsHiSeqMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = OMGTrackMetadata()

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            fld('facility_sample_id', 'facility_sample_id'),
            fld('bpa_id', 'bpa_sample_id', coerce=ingest_utils.extract_bpa_id),
            fld('bpa_dataset_id', 'bpa_dataset_id'),
            fld('bpa_work_order', 'bpa_work_order'),
            fld('file', 'file'),
            fld('library_prep_method', 'library_prep_method'),
            fld('sequencing_facility', 'sequencing_facility'),
            fld('sequencing_platform', 'sequencing_platform'),
            fld('software_version', 'software_version'),
            fld('omg_project', 'omg_project'),
            fld('data_custodian', 'data_custodian'),
        ]
        try:
            wrapper = ExcelWrapper(
                field_spec,
                fname,
                sheet_name=None,
                header_length=1,
                column_name_row_index=0,
                formatting_info=True,
                additional_context=metadata_info[os.path.basename(fname)])
            rows = list(wrapper.get_all())
            return rows
        except:
            logger.error("Cannot parse: `%s'" % (fname))
            return []

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
                objs[obj['bpa_id']].append(obj)

            for bpa_id, row_objs in list(objs.items()):
                obj = common_values(row_objs)
                track_meta = self.track_meta.get(obj['ticket'])

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)
                bpa_id = obj['bpa_id']
                if bpa_id is None:
                    continue
                name = bpa_id_to_ckan_name(bpa_id, self.ckan_data_type, flow_id)
                context = {}
                for contextual_source in self.contextual_metadata:
                    context.update(contextual_source.get(bpa_id))
                obj.update({
                    'name': name,
                    'id': name,
                    'flow_id': flow_id,
                    'title': 'OMG Genomics HiSeq Raw %s %s' % (bpa_id, flow_id),
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
            for filename, md5, file_info in files.parse_md5_file(md5_file, [files.hiseq_filename_re]):
                # FIXME: we should upload these somewhere centrally
                if filename.endswith('_metadata.xlsx'):
                    continue
                if file_info is None:
                    logger.debug("unable to parse filename: `%s'" % (filename))
                    continue
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((ingest_utils.extract_bpa_id(resource['bpa_id']), resource['flow_cell_id']), legacy_url, resource))
        return resources
