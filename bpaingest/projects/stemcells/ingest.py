from __future__ import print_function

from unipath import Path

from ...libs import ingest_utils
from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...libs.excel_wrapper import ExcelWrapper
from .tracking import StemcellTrackMetadata
from .contextual import (
    StemcellAGRFTranscriptomeContextual,
    StemcellAGRFsmRNAContextual,
    StemcellRamaciottiSingleCell,
    StemcellMetabolomicsContextual)
from . import files
from glob import glob

import os
import re

logger = make_logger(__name__)


def fix_analytical_platform(s):
    if s == 'LC-MS' or s == 'GC-MS':
        return s
    if s == 'LCMS':
        return 'LC-MS'
    if s == 'GCMS':
        return 'GC-MS'
    raise Exception("invalid metabolomics analytical platform: `%s'" % (s))


class StemcellsTranscriptomeMetadata(BaseMetadata):
    contextual_classes = [StemcellAGRFTranscriptomeContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/transcriptome/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5', r'^.*_metadata\.xlsx']
    organization = 'bpa-stemcells'
    auth = ('stemcell', 'stemcell')
    ckan_data_type = 'stemcells-transcriptomics'

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellTrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
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
            formatting_info=True,
            additional_context=additional_context)
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
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            all_rows.update(StemcellsTranscriptomeMetadata.parse_spreadsheet(fname, xlsx_info))
        for row in sorted(all_rows):
            bpa_id = row.bpa_id
            if bpa_id is None:
                continue
            obj = {}
            name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
            track_meta = self.track_meta.get(row.ticket)
            obj.update({
                'name': name,
                'id': name,
                'bpa_id': bpa_id,
                'notes': 'Stemcell Transcriptomics %s' % (bpa_id),
                'title': 'Stemcell Transcriptomics %s' % (bpa_id),
                'omics': 'transcriptomics',
                'data_generated': 'True',
                'insert_size_range': row.insert_size_range,
                'library_construction_protocol': row.library_construction_protocol,
                'sequencer': row.sequencer,
                'analysis_software_version': row.analysis_software_version,
                'ticket': row.ticket,
                'facility': row.facility_code.upper(),
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(bpa_id))
            tag_names = ['transcriptome']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.transcriptome_filename_re):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = bpa_mirror_url('bpa/stemcell/raw/transcriptome/%(facility_code)s/%(ticket)s/' % xlsx_info + filename)
                resources.append(((bpa_id,), legacy_url, resource))
        return resources


class StemcellsSmallRNAMetadata(BaseMetadata):
    contextual_classes = [StemcellAGRFsmRNAContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/small_rna/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5', r'^.*_metadata\.xlsx']
    organization = 'bpa-stemcells'
    auth = ('stemcell', 'stemcell')
    ckan_data_type = 'stemcells-smallrna'

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellTrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
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
            formatting_info=True,
            additional_context=additional_context)
        rows = list(wrapper.get_all())
        return rows

    def get_packages(self):
        logger.info("Ingesting Stemcells SmallRNA metadata from {0}".format(self.path))
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and BPA_ID is the primary key
        all_rows = set()
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Stemcells SmallRNA metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            all_rows.update(StemcellsSmallRNAMetadata.parse_spreadsheet(fname, xlsx_info))
        for row in sorted(all_rows):
            bpa_id = row.bpa_id
            if bpa_id is None:
                continue
            obj = {}
            name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
            track_meta = self.track_meta.get(row.ticket)
            obj.update({
                'name': name,
                'id': name,
                'bpa_id': bpa_id,
                'notes': 'Stemcell SmallRNA %s' % (bpa_id),
                'title': 'Stemcell SmallRNA %s' % (bpa_id),
                'omics': 'transcriptomics',
                'data_generated': 'True',
                'insert_size_range': row.insert_size_range,
                'library_construction_protocol': row.library_construction_protocol,
                'sequencer': row.sequencer,
                'analysis_software_version': row.analysis_software_version,
                'ticket': row.ticket,
                'facility': row.facility_code.upper(),
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(bpa_id))
            tag_names = ['small-rna']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.smallrna_filename_re):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = bpa_mirror_url('bpa/stemcell/raw/small_rna/%(facility_code)s/%(ticket)s/' % xlsx_info + filename)
                resources.append(((bpa_id,), legacy_url, resource))
        return resources


class StemcellsSingleCellRNASeqMetadata(BaseMetadata):
    contextual_classes = [StemcellRamaciottiSingleCell]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/single_cell_rnaseq/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5', r'^.*_metadata\.xlsx']
    organization = 'bpa-stemcells'
    auth = ('stemcell', 'stemcell')
    ckan_data_type = 'stemcells-singlecellrnaseq'
    resource_linkage = ('bpa_id_range',)

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellTrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
        def parse_bpa_id_range(s):
            return s.strip().split('/')[-1]

        field_spec = [
            ("bpa_id_range", re.compile(r'^.*sample unique id$'), parse_bpa_id_range),
            ("sample_extaction_id", "Sample extraction ID", None),
            ("insert_size_range", "Insert size range", None),
            ("library_construction_protocol", "Library construction protocol", None),
            ("sequencer", "Sequencer", None),
            ("fastq_generation", "Fastq generation", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=additional_context)
        rows = list(wrapper.get_all())
        return rows

    def get_packages(self):
        logger.info("Ingesting Stemcells SingleCellRNASeq metadata from {0}".format(self.path))
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and BPA_ID is the primary key
        all_rows = set()
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Stemcells SingleCellRNASeq metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            all_rows.update(StemcellsSingleCellRNASeqMetadata.parse_spreadsheet(fname, xlsx_info))
        for row in sorted(all_rows):
            bpa_id_range = row.bpa_id_range
            if bpa_id_range is None:
                continue
            obj = {}
            name = bpa_id_to_ckan_name(bpa_id_range, self.ckan_data_type)
            track_meta = self.track_meta.get(row.ticket)
            # NB: this isn't really the BPA ID, it's the first BPA ID
            bpa_id = ingest_utils.extract_bpa_id(bpa_id_range.split('-', 1)[0])
            obj.update({
                'name': name,
                'id': name,
                'bpa_id': bpa_id,
                'bpa_id_range': bpa_id_range,
                'notes': 'Stemcell SingleCellRNASeq %s' % (bpa_id_range),
                'title': 'Stemcell SingleCellRNASeq %s' % (bpa_id_range),
                'insert_size_range': row.insert_size_range,
                'library_construction_protocol': row.library_construction_protocol,
                'sequencer': row.sequencer,
                'fastq_generation': row.fastq_generation,
                'ticket': row.ticket,
                'facility': row.facility_code.upper(),
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            for contextual_source in self.contextual_metadata:
                # NB: the rows in the contextual metadata are all identical across the range, so this works
                obj.update(contextual_source.get(bpa_id))
            tag_names = ['single-cell-rnaseq']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.singlecell_filename_re):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                bpa_id_range = file_info.get('id')
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = bpa_mirror_url('bpa/stemcell/raw/single_cell_rnaseq/%(facility_code)s/%(ticket)s/' % xlsx_info + filename)
                resources.append(((bpa_id_range,), legacy_url, resource))
        return resources


class StemcellsMetabolomicMetadata(BaseMetadata):
    contextual_classes = [StemcellMetabolomicsContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/metabolomic/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5', r'^.*_metadata.*\.xlsx']
    organization = 'bpa-stemcells'
    auth = ('stemcell', 'stemcell')
    resource_linkage = ('bpa_id', 'analytical_platform')
    ckan_data_type = 'stemcells-metabolomic'

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellTrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
        field_spec = [
            ("bpa_id", re.compile(r'^.*sample unique id$'), ingest_utils.extract_bpa_id),
            ("sample_fractionation_extraction_solvent", "sample fractionation / extraction solvent", None),
            ("analytical_platform", "platform", None),
            ("instrument_column_type", "instrument/column type", None),
            ("method", "Method", None),
            ("mass_spectrometer", "Mass Spectrometer", None),
            ("acquisition_mode", "acquisition mode", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=additional_context)
        rows = list(wrapper.get_all())
        return rows

    def get_packages(self):
        logger.info("Ingesting Stemcells Metabolomics metadata from {0}".format(self.path))
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and BPA_ID is the primary key
        all_rows = set()
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Stemcells Metabolomics metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            all_rows.update(StemcellsMetabolomicMetadata.parse_spreadsheet(fname, xlsx_info))
        for row in sorted(all_rows):
            bpa_id = row.bpa_id
            if bpa_id is None:
                continue
            obj = {}
            name = bpa_id_to_ckan_name(bpa_id.split('.')[-1] + '-' + row.analytical_platform, self.ckan_data_type)
            track_meta = self.track_meta.get(row.ticket)
            analytical_platform = fix_analytical_platform(row.analytical_platform)
            obj.update({
                'name': name,
                'id': name,
                'bpa_id': bpa_id,
                'notes': 'Stemcell Metabolomics %s %s' % (bpa_id, analytical_platform),
                'title': 'Stemcell Metabolomics %s %s' % (bpa_id, analytical_platform),
                'omics': 'transcriptomics',
                'data_generated': 'True',
                'sample_fractionation_extraction_solvent': row.sample_fractionation_extraction_solvent,
                'analytical_platform': analytical_platform,
                'instrument_column_type': row.instrument_column_type,
                'method': row.method,
                'mass_spectrometer': row.mass_spectrometer,
                'acquisition_mode': row.acquisition_mode,
                'ticket': row.ticket,
                'facility': row.facility_code.upper(),
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(bpa_id, analytical_platform))
            tag_names = ['metabolomic', analytical_platform]
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.metabolomics_filename_re):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['analytical_platform'] = fix_analytical_platform(resource['analytical_platform'])
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = bpa_mirror_url('bpa/stemcell/raw/metabolomic/%(facility_code)s/%(ticket)s/' % xlsx_info + filename)
                resources.append(((bpa_id, resource['analytical_platform']), legacy_url, resource))
        return resources


class StemcellsProteomicMetadata(BaseMetadata):
    contextual_classes = []
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/raw/proteomic/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5', r'^.*_metadata\.xlsx']
    organization = 'bpa-stemcells'
    auth = ('stemcell', 'stemcell')
    ckan_data_type = 'stemcells-proteomic'

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = StemcellTrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
        field_spec = [
            ("bpa_id", re.compile(r'^.*sample unique id$'), ingest_utils.extract_bpa_id),
            ("facility", 'facility', None),
            ("sample_fractionation", 'sample fractionation (none/number)', None),
            ("lc_column_type", 'lc/column type', None),
            ("gradient_time", re.compile(r'gradient time (min).*'), None),
            ("sample_on_column", 'sample on column (g)', None),
            ("mass_spectrometer", 'mass spectrometer', None),
            ("acquisition_mode", 'acquisition mode / fragmentation', None),
            ("raw_filename", 'raw file name', None),
            ("protein_result_filename", 'protein result filename', None),
            ("peptide_result_filename", 'peptide result filename', None),
            ("database", 'database', None),
            ("database_size", 'database size', None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=1,
            column_name_row_index=0,
            formatting_info=True,
            additional_context=additional_context)
        rows = list(wrapper.get_all())
        return rows

    def get_packages(self):
        logger.info("Ingesting Stemcells Proteomics metadata from {0}".format(self.path))
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and BPA_ID is the primary key
        all_rows = set()
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Stemcells Proteomics metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            all_rows.update(StemcellsProteomicMetadata.parse_spreadsheet(fname, xlsx_info))
        bpa_id_ticket_facility = dict((t.bpa_id, (t.ticket, t.facility_code)) for t in all_rows)
        self.filename_metadata = {}
        self.filename_metadata.update(
            dict((t.raw_filename, t) for t in all_rows))
        self.filename_metadata.update(
            dict((t.protein_result_filename, t) for t in all_rows))
        self.filename_metadata.update(
            dict((t.peptide_result_filename, t) for t in all_rows))
        for bpa_id, (ticket, facility_code) in sorted(bpa_id_ticket_facility.items()):
            if bpa_id is None:
                continue
            obj = {}
            name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
            track_meta = self.track_meta.get(ticket)
            obj.update({
                'name': name,
                'id': name,
                'bpa_id': bpa_id,
                'notes': 'Stemcell Proteomics %s' % (bpa_id),
                'title': 'Stemcell Proteomics %s' % (bpa_id),
                'omics': 'proteomics',
                'data_generated': 'True',
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'ticket': ticket,
                'facility': facility_code,
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            # for contextual_source in self.contextual_metadata:
            #     obj.update(contextual_source.get(bpa_id, track_meta))
            tag_names = ['proteomic']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.proteomics_filename_re):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource_meta = self.filename_metadata.get(filename, {})
                for k in ("sample_fractionation", "lc_column_type", "gradient_time", "sample_on_column", "mass_spectrometer", "acquisition_mode", "database", "database_size"):
                    resource[k] = getattr(resource_meta, k)
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = bpa_mirror_url('bpa/stemcell/raw/proteomic/%(facility_code)s/%(ticket)s/' % xlsx_info + filename)
                resources.append(((bpa_id, ), legacy_url, resource))
        return resources
