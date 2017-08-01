# -*- coding: utf-8 -*-

from unipath import Path
from collections import defaultdict
from urllib.parse import urljoin
from hashlib import md5 as md5hash

from ...libs import ingest_utils
from ...libs.md5lines import md5lines
from ...util import make_logger, bpa_id_to_ckan_name, csv_to_named_tuple, common_values, clean_tag_name
from ...abstract import BaseMetadata
from ...libs.excel_wrapper import ExcelWrapper
from glob import glob
from .tracking import (
    SepsisTrackMetadata,
    SepsisGenomicsTrackMetadata,
    SepsisGoogleTrackMetadata)
from .contextual import (
    SepsisBacterialContextual,
    SepsisGenomicsContextual,
    SepsisMetabolomicsLCMSContextual,
    SepsisProteomicsSwathMSContextual,
    SepsisProteomicsMS1QuantificationContextual,
    SepsisTranscriptomicsHiseqContextual)
from . import files
import os

logger = make_logger(__name__)


def sepsis_contextual_tags(cls, obj):
    tags = [cls.omics, cls.technology]
    taxon = obj.get('taxon_or_organism')
    strain = obj.get('strain_or_isolate')
    if taxon and strain:
        tags.append(clean_tag_name(('%s_%s' % (taxon, strain)).replace(' ', '_')))
    data_type = obj.get('data_type')
    if data_type:
        tags.append(clean_tag_name(data_type))
    growth_media = obj.get('growth_media')
    if growth_media:
        tags.append(clean_tag_name(growth_media))
    return tags


class BaseSepsisMetadata(BaseMetadata):
    # printable names used when generating CKAN schemas
    # package_field: printable name
    package_field_names = {
        'growth_condition_time': 'growth_condition_time_(h)'
    }

    def __init__(self, *args, **kwargs):
        super(BaseSepsisMetadata, self).__init__(*args, **kwargs)
        self.google_track_meta = SepsisGoogleTrackMetadata()


class SepsisGenomicsMiseqMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisGenomicsContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/raw/miseq/']
    metadata_url_components = ('facility_code', 'ticket')
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-genomics-miseq'
    omics = 'genomics'
    technology = 'miseq'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisGenomicsMiseqMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisGenomicsTrackMetadata('GenomicsMiSeq')
        self.metadata_info = metadata_info

    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", ingest_utils.extract_bpa_id),
            ("insert_size_range", "Insert size range", None),
            ("library_construction_protocol", "Library construction protocol", None),
            ("sequencer", "Sequencer", None),
            ("analysis_software_version", "AnalysisSoftwareVersion", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=self.metadata_info[os.path.basename(fname)])
        return wrapper.get_all()

    def _get_packages(self):
        logger.info("Ingesting Sepsis Genomics Miseq metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Genomics metadata file {0}".format(fname))
            rows = list(self.parse_spreadsheet(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                bpa_id = row.bpa_id
                track_meta = self.bpam_track_meta.get(bpa_id)
                obj = track_meta.copy()
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(google_track_meta.date_of_transfer_to_archive),
                    'notes': 'ARP Genomics Miseq Data: %s %s' % (track_meta['taxon_or_organism'], track_meta['strain_or_isolate']),
                    'title': 'Sepsis Genomics Miseq %s' % (bpa_id),
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'analysis_software_version': row.analysis_software_version,
                    'type': self.ckan_data_type,
                    'data_generated': True,
                    'private': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id, track_meta))
                tag_names = sepsis_contextual_tags(self, obj)
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file({'miseq': [files.miseq_filename_re]}, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('index', 'lane', 'vendor', 'read', 'flow_cell_id', 'library', 'extraction', 'runsamplenum'))
                resource['seq_size'] = file_info.get('size')
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                resource['resource_type'] = self.ckan_data_type
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], file_info.filename)
                resources.append(((bpa_id,), legacy_url, resource))
        return resources


class SepsisGenomicsPacbioMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisGenomicsContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/raw/pacbio/']
    metadata_url_components = ('facility_code', 'ticket')
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-genomics-pacbio'
    omics = 'genomics'
    technology = 'pacbio'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisGenomicsPacbioMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisGenomicsTrackMetadata('GenomicsPacBio')
        self.metadata_info = metadata_info

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('SepsisGenomicsPacbioTrack', fname)
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", ingest_utils.extract_bpa_id),
            ("insert_size_range", "Insert size range", None),
            ("library_construction_protocol", "Library construction protocol", None),
            ("sequencer", "Sequencer", None),
            ("sequencer_run_id", "Run ID", None),
            ("smrt_cell_id", "SMRT Cell ID", None),
            ("cell_position", ("Cell Postion", "Cell Position"), None),
            ("rs_version", "RS Version", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=self.metadata_info[os.path.basename(fname)])
        return wrapper.get_all()

    def _get_packages(self):
        logger.info("Ingesting Sepsis Genomics Pacbio metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Genomics metadata file {0}".format(fname))
            rows = list(self.parse_spreadsheet(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                bpa_id = row.bpa_id
                track_meta = self.bpam_track_meta.get(bpa_id)
                obj = track_meta.copy()
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(google_track_meta.date_of_transfer_to_archive),
                    'title': 'Sepsis Genomics Pacbio %s' % (bpa_id),
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'notes': 'ARP Genomics Pacbio Data: %s %s' % (track_meta['taxon_or_organism'], track_meta['strain_or_isolate']),
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'sequencer_run_id': row.sequencer_run_id,
                    'smrt_cell_id': row.smrt_cell_id,
                    'cell_position': row.cell_position,
                    'rs_version': row.rs_version,
                    'type': self.ckan_data_type,
                    'private': True,
                    'data_generated': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id, track_meta))
                tag_names = sepsis_contextual_tags(self, obj)
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file({'pacbio': [files.pacbio_filename_re]}, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('run_id', 'vendor', 'data_type', 'machine_data'))
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                resource['resource_type'] = self.ckan_data_type
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], file_info.filename)
                resources.append(((bpa_id,), legacy_url, resource))
        return resources


class SepsisTranscriptomicsHiseqMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisTranscriptomicsHiseqContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/transcriptomics/raw/hiseq/']
    metadata_url_components = ('facility_code', 'ticket')
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-transcriptomics-hiseq'
    omics = 'transcriptomics'
    technology = 'hiseq'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisTranscriptomicsHiseqMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisTrackMetadata('TranscriptomicsHiSeq')
        self.metadata_info = metadata_info

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisGenomicsHiseqTrack', fname)
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", "Antibiotic Resistant Pathogen sample unique ID", ingest_utils.extract_bpa_id),
            ("sample", "Sample (MGR code)", None),
            ("library_construction_protocol", "Library construction protocol", None),
            ("barcode_tag", "Barcode tag", None),
            ("sequencer", "Sequencer", None),
            ("casava_version", "CASAVA version", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=self.metadata_info[os.path.basename(fname)])
        return wrapper.get_all()

    def _get_packages(self):
        logger.info("Ingesting Sepsis Transcriptomics Hiseq metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Transcriptomics metadata file {0}".format(fname))
            rows = list(self.parse_spreadsheet(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                track_meta = self.bpam_track_meta.get(bpa_id)
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
                obj = track_meta.copy()
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(google_track_meta.date_of_transfer_to_archive),
                    'title': 'ARP Transcriptomics Hiseq %s' % (bpa_id),
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'notes': 'ARP Transcriptomics Hiseq Data: %s %s' % (track_meta['taxon_or_organism'], track_meta['strain_or_isolate']),
                    'sample': row.sample,
                    'library_construction_protocol': row.library_construction_protocol,
                    'barcode_tag': row.barcode_tag,
                    'sequencer': row.sequencer,
                    'casava_version': row.casava_version,
                    'type': self.ckan_data_type,
                    'private': True,
                    'data_generated': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id, track_meta))
                tag_names = sepsis_contextual_tags(self, obj)
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file({'hiseq': [files.hiseq_filename_re]}, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('library', 'vendor', 'flow_cell_id', 'index', 'lane', 'read'))
                resource['seq_size'] = file_info.get('size')
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                resource['resource_type'] = self.ckan_data_type
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], file_info.filename)
                resources.append(((bpa_id,), legacy_url, resource))
        return resources


class SepsisMetabolomicsLCMSMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisMetabolomicsLCMSContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/metabolomics/raw/lcms/']
    metadata_url_components = ('facility_code', 'ticket')
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-metabolomics-lcms'
    omics = 'metabolomics'
    technology = 'lcms'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisMetabolomicsLCMSMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisTrackMetadata('MetabolomicsLCMS')
        self.metadata_info = metadata_info

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisMetabolomicsLCMSTrack', fname)
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", ingest_utils.extract_bpa_id),
            ("sample_fractionation_extract_solvent", "Sample fractionation / Extraction Solvent", None),
            ("lc_column_type", "LC/column type", None),
            ("gradient_time_min_flow", "Gradient time (min) / flow", None),
            ("mass_spectrometer", "Mass Spectrometer", None),
            ("acquisition_mode", "Acquisition Mode", None),
            ("raw_file_name", "Raw file name", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=1,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=self.metadata_info[os.path.basename(fname)])
        return wrapper.get_all()

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Metabolomics LCMS metadata file {0}".format(fname))
            rows = list(self.parse_spreadsheet(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                track_meta = self.bpam_track_meta.get(bpa_id)
                obj = track_meta.copy()
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(google_track_meta.date_of_transfer_to_archive),
                    'title': 'ARP Metabolomics LCMS %s' % (bpa_id),
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'notes': 'ARP Metabolomics LCMS Data: %s %s' % (track_meta['taxon_or_organism'], track_meta['strain_or_isolate']),
                    'sample_fractionation_extract_solvent': row.sample_fractionation_extract_solvent,
                    'lc_column_type': row.lc_column_type,
                    'gradient_time_min_flow': row.gradient_time_min_flow,
                    'mass_spectrometer': row.mass_spectrometer,
                    'acquisition_mode': row.acquisition_mode,
                    'raw_file_name': row.raw_file_name,
                    'type': self.ckan_data_type,
                    'private': True,
                    'data_generated': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id, track_meta))
                tag_names = sepsis_contextual_tags(self, obj)
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file({'lcms': [files.metabolomics_lcms_filename_re]}, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('vendor', 'platform', 'mastr_ms_id', 'machine_data'))
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                resource['resource_type'] = self.ckan_data_type
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], file_info.filename)
                resources.append(((bpa_id,), legacy_url, resource))
        return resources


class SepsisProteomicsMS1QuantificationMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsMS1QuantificationContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/raw/ms1quantification/']
    metadata_url_components = ('facility_code', 'ticket')
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-proteomics-ms1quantification'
    omics = 'proteomics'
    technology = 'ms1quantification'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisProteomicsMS1QuantificationMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisTrackMetadata('ProteomicsMS1Quantification')
        self.metadata_info = metadata_info

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisProteomicsMS1QuantificationTrack', fname)
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", ingest_utils.extract_bpa_id),
            ("facility", "Facility", None),
            ("sample_fractionation_none_number", "Sample fractionation (none/number)", None),
            ("lc_column_type", "LC/column type", None),
            ("gradient_time_per_acn", "Gradient time (min)  /  % ACN (start-finish main gradient) / flow", None),
            ("sample_on_column", "sample on column (g)", None),  # Note: unicode micro stripped out
            ("mass_spectrometer", "Mass Spectrometer", None),
            ("acquisition_mode_fragmentation", "Acquisition Mode / fragmentation", None),
            ("raw_file_name", "Raw file name", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=1,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=self.metadata_info[os.path.basename(fname)])
        return wrapper.get_all()

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Proteomics MS1Quantification metadata file {0}".format(fname))
            rows = list(self.parse_spreadsheet(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                bpam_track_meta = self.bpam_track_meta.get(bpa_id)
                obj = bpam_track_meta.copy()
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'title': 'ARP Proteomics MS1Quantification %s' % (bpa_id),
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(google_track_meta.date_of_transfer_to_archive),
                    'notes': 'ARP Proteomics MS1Quantification Data: %s %s' % (bpam_track_meta['taxon_or_organism'], bpam_track_meta['strain_or_isolate']),
                    'sample_fractionation_none_number': row.sample_fractionation_none_number,
                    'lc_column_type': row.lc_column_type,
                    'gradient_time_per_acn': row.gradient_time_per_acn,
                    'sample_on_column': row.sample_on_column,
                    'mass_spectrometer': row.mass_spectrometer,
                    'acquisition_mode_fragmentation': row.acquisition_mode_fragmentation,
                    'raw_file_name': row.raw_file_name,
                    'type': self.ckan_data_type,
                    'private': True,
                    'data_generated': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id, bpam_track_meta))
                tag_names = sepsis_contextual_tags(self, obj)
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file({'ms1quantification': [files.proteomics_ms1quantification_filename_re]}, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('vendor', 'machine_data'))
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                resource['resource_type'] = self.ckan_data_type
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], file_info.filename)
                resources.append(((bpa_id,), legacy_url, resource))
        return resources


class SepsisProteomicsSwathMSBaseSepsisMetadata(BaseSepsisMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsSwathMSContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/raw/swathms/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata\.xlsx']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    omics = 'proteomics'
    technology = 'swathms'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisProteomicsSwathMSBaseSepsisMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info
        self.contextual_metadata = contextual_metadata
        self.bpam_track_meta = SepsisTrackMetadata('ProteomicsSwathMS')
        self.package_data, self.file_data = self.get_spreadsheet_data()

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisProteomicsSwathMSTrack', fname)
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    def parse_spreadsheet(self, fname):
        def parse_pooled_bpa_id(s):
            if isinstance(s, str) and ',' in s:
                return tuple([ingest_utils.extract_bpa_id(t.strip()) for t in s.split(',')])
            else:
                return ingest_utils.extract_bpa_id(s)

        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", parse_pooled_bpa_id),
            ("facility", "Facility", None),
            ("sample_fractionation_none_number", "Sample fractionation (none/number)", None),
            ("lc_column_type", "LC/column type", None),
            ("gradient_time_per_acn", "Gradient time (min)  /  % ACN (start-finish main gradient) / flow", None),
            ("sample_on_column", "sample on column (g)", None),  # Note: unicode micro stripped out
            ("mass_spectrometer", "Mass Spectrometer", None),
            ("acquisition_mode_fragmentation", "Acquisition Mode / fragmentation", None),
            ("raw_file_name", "Raw file name", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=1,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=self.metadata_info[os.path.basename(fname)])
        return wrapper.get_all()

    def get_spreadsheet_data(self):
        """
        proteomics SWATH is a bit different, the spreadsheets might have dupes by `bpa id`,
        so some data in the sheet is per-file and some is per-ID. the only way to go from
        filename back to the pool/bpa_id is via the spreadsheet, so we also need to build
        that mapping
        """
        package_data = {}
        file_data = {}
        for fname in glob(self.path + '/*_metadata.xlsx'):
            logger.info("Processing Sepsis Proteomics SwathMS metadata file {0}".format(fname))
            rows = list(self.parse_spreadsheet(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            google_track_meta = self.google_track_meta.get(ticket)
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                contextual_meta = {}
                # if `bpa_id` is a tuple, we've got a pooled sample
                if type(bpa_id) is tuple:
                    data_type = '2d'
                    printable_bpa_id = '_'.join([t.split('.')[-1] for t in sorted(bpa_id)])
                    track_meta = common_values([self.bpam_track_meta.get(t) for t in bpa_id])
                    for contextual_source in self.contextual_metadata:
                        contextual_meta.update(common_values([contextual_source.get(t, track_meta) for t in bpa_id]))
                else:
                    data_type = '1d'
                    printable_bpa_id = bpa_id
                    track_meta = self.bpam_track_meta.get(bpa_id)
                    for contextual_source in self.contextual_metadata:
                        contextual_meta.update(contextual_source.get(bpa_id, track_meta))
                name = bpa_id_to_ckan_name(printable_bpa_id.split('.')[-1], self.ckan_data_type)
                package_meta = {
                    'lc_column_type': row.lc_column_type,
                    'gradient_time_per_acn': row.gradient_time_per_acn,
                    'mass_spectrometer': row.mass_spectrometer,
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(google_track_meta.date_of_transfer_to_archive),
                }
                package_meta.update(contextual_meta)
                package_data[name] = (name, data_type, printable_bpa_id, track_meta, package_meta)
                file_data[row.raw_file_name] = {
                    'package_name': printable_bpa_id,
                    'sample_fractionation_none_number': row.sample_fractionation_none_number,
                    'sample_on_column': row.sample_on_column,
                    'acquisition_mode_fragmentation': row.acquisition_mode_fragmentation,
                }
        return package_data, file_data

    def get_swath_packages(self, data_type):
        packages = []
        for package_name, (name, package_data_type, printable_bpa_id, track_meta, submission_meta) in list(self.package_data.items()):
            if package_data_type != data_type:
                continue
            obj = track_meta.copy()
            obj.update(submission_meta)
            pool = ''
            if data_type == '1d':
                obj.update({
                    'bpa_id': printable_bpa_id,
                })
            if data_type == '2d':
                pool = 'Pool '
                obj.update({
                    'pool_bpa_ids': printable_bpa_id,
                })
            obj.update({
                'name': name,
                'id': name,
                'title': 'ARP Proteomics SwathMS %s%s' % (pool, printable_bpa_id),
                'notes': 'ARP Proteomics SwathMS %sData: %s %s' % (pool, track_meta['taxon_or_organism'], track_meta['strain_or_isolate']),
                'type': self.ckan_data_type,
                'private': True,
                'data_generated': True,
            })
            tag_names = sepsis_contextual_tags(self, obj)
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_swath_resources(self, data_type):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []

        swath_patterns = {
            '1d': [
                files.proteomics_swathms_1d_ida_filename_re,
                files.proteomics_swathms_lib_filename_re,
                files.proteomics_swathms_swath_raw_filename_re
            ],
            '2d': [
                files.proteomics_swathms_lib_filename_re,
                files.proteomics_swathms_2d_ida_filename_re,
                files.proteomics_swathms_mspeak_filename_re,
                files.proteomics_swathms_msresult_filename_re,
            ]
        }
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(swath_patterns, md5_file):
                if file_info.data_type != data_type:
                    continue
                resource = dict((t, file_info.get(t)) for t in ('vendor', 'machine_data'))
                if file_info.filename not in self.file_data:
                    logger.warning("no submission metadata for `%s'" % (file_info.filename))
                file_meta = self.file_data.get(file_info.filename, {})
                resource['md5'] = resource['id'] = file_info.md5
                resource['data_type'] = file_info.get('type')
                resource['vendor'] = file_info.get('vendor')
                resource['resource_type'] = data_type
                package_name = file_meta.pop('package_name', None)
                resource.update(file_meta)
                resource['name'] = file_info.filename
                if file_info.data_type == '1d':
                    package_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                elif file_info.data_type == '2d':
                    package_id = package_name
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], file_info.filename)
                resources.append(((package_id,), legacy_url, resource))
        return resources


class SepsisProteomicsSwathMSCombinedSampleMetadata(BaseSepsisMetadata):
    """
    see https://github.com/muccg/bpa-archive-ops/issues/163
    this is a one-off, for a single dataset that was inadvertantly produced in this way
    as such, this ingest is deliberately minimal
    """
    contextual_classes = []
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/raw/swathms-combined-sample/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata\.xlsx$']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-proteomics-swathms-combined-sample'
    resource_linkage = ('folder_name',)
    omics = 'proteomics'
    technology = 'swathms-combined-sample'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisProteomicsSwathMSCombinedSampleMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata()

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
        def make_bpa_id_list(s):
            return tuple([ingest_utils.extract_bpa_id(t.strip()) for t in s.split(',')])

        field_spec = [
            ('bpa_id_list', 'bacterial sample unique id', make_bpa_id_list),
            ('facility', 'facility'),
            ('sample_fractionation_none_number', 'sample fractionation (none/number)'),
            ('lc_column_type', 'lc/column type'),
            ('gradient_time_min', 'gradient time (min)  /  % acn (start-finish main gradient) / flow'),
            ('sample_on_column_ug', 'sample on column (g)'),
            ('mass_spectrometer', 'mass spectrometer'),
            ('acquisition_mode_fragmentation', 'acquisition mode / fragmentation'),
            ('raw_file_name', 'raw file name'),
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

    def _get_packages(self):
        logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, xlsx_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode('utf8')).hexdigest()
            name = bpa_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            obj.update({
                'name': name,
                'id': name,
                'notes': '%s' % (folder_name),
                'title': '%s' % (folder_name),
                'omics': 'proteomics',
                'data_generated': 'True',
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            tag_names = sepsis_contextual_tags(self, obj)
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        # one MD5 file per 'folder_name', so we just take every file and upload
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            with open(md5_file) as fd:
                for md5, filename in md5lines(fd):
                    resource = {}
                    resource['md5'] = resource['id'] = md5
                    resource['name'] = filename
                    xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                    folder_name = self.google_track_meta.get(xlsx_info['ticket']).folder_name
                    legacy_url = urljoin(xlsx_info['base_url'], filename)
                    resources.append(((folder_name,), legacy_url, resource))
        return resources


class SepsisProteomicsSwathMSMetadata(SepsisProteomicsSwathMSBaseSepsisMetadata):
    ckan_data_type = 'arp-proteomics-swathms'

    def _get_packages(self):
        return self.get_swath_packages('1d')

    def _get_resources(self):
        return self.get_swath_resources('1d')


class SepsisProteomicsSwathMSPoolMetadata(SepsisProteomicsSwathMSBaseSepsisMetadata):
    ckan_data_type = 'arp-proteomics-swathms-pool'
    resource_linkage = ('pool_bpa_ids',)

    def _get_packages(self):
        return self.get_swath_packages('2d')

    def _get_resources(self):
        return self.get_swath_resources('2d')


class BaseSepsisAnalysedMetadata(BaseSepsisMetadata):
    def apply_common_context(self, obj, bpa_ids):
        # find the contextual metadata in common between these BPA IDs
        context_objs = []
        for bpa_id in bpa_ids:
            context_obj = {}
            for contextual_source in self.contextual_metadata:
                context_obj.update(contextual_source.get(bpa_id, obj))
            context_objs.append(context_obj)
        obj.update(common_values(context_objs))
        # find the tracking metadata in common between these BPA IDs
        tracking_objs = []
        for bpa_id in bpa_ids:
            tracking_obj = {}
            for bpam_source in self.bpam_track_meta:
                tracking_obj.update(bpam_source.get(bpa_id))
            tracking_objs.append(tracking_obj)
        obj.update(common_values(tracking_objs))
        return obj

    @classmethod
    def google_drive_track_to_object(cls, trk):
        "copy over the relevant bits of a sepsis google drive track object, to a package object"
        obj = {
            'facility': trk.omics_facility,
            'data_type': trk.data_type_pre_pilot_pilot_or_main_dataset,
            'ticket': trk.ccg_jira_ticket
        }
        for fld in ('date_of_transfer', 'taxon_or_organism', 'strain_or_isolate', 'growth_media', 'folder_name', 'date_of_transfer_to_archive', 'file_count'):
            obj[fld] = getattr(trk, fld)
        return obj


class SepsisProteomicsAnalysedMetadata(BaseSepsisAnalysedMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsSwathMSContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/analysed/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata\.xlsx$']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-proteomics-analysed'
    resource_linkage = ('folder_name',)
    omics = 'proteomics'
    technology = 'analysed'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisProteomicsAnalysedMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata()
        self.bpam_track_meta = [SepsisTrackMetadata('ProteomicsMS1Quantification'), SepsisTrackMetadata('ProteomicsSwathMS')]

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
        field_spec = [
            ('data_analysis_date', 'data analysis date (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('facility_project_code_experiment_code', 'facility project code_facility experiment code'),
            ('bpa_id', 'sample name (5 digit bpa id)', ingest_utils.extract_bpa_id),
            ('taxon_or_organism', 'taxon_or_organism'),
            ('strain_or_isolate', 'strain_or_isolate'),
            ('serovar', 'serovar'),
            ('growth_media', 'growth media'),
            ('replicate', 'replicate', ingest_utils.get_int),
            ('growth_condition_time', 'growth_condition_time'),
            ('growth_condition_growth', 'growth_condition_growth phase'),
            ('growth_condition_od600', 'growth_condition_od600 reading'),
            ('growth_condition_temperature', 'growth_condition_temperature'),
            ('growth_condition_media', 'growth_condition_media'),
            ('omics', 'omics'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('data_type', 'data type'),
            ('zip_file_name', 'file name of analysed data (folder or zip file)'),
            ('genome_used', 'genome used (file name if used annoted bpa genome)'),
            ('database', 'database (if used publicly available genome)'),
            ('version', 'version (genome or database)'),
            ('translation', 'translation (3 frame or 6 frame)'),
            ('proteome_size', 'proteome size'),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=8,
            column_name_row_index=7,
            formatting_info=True,
            additional_context=additional_context)
        rows = list(wrapper.get_all())
        return rows

    def _get_packages(self):
        logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, xlsx_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode('utf8')).hexdigest()
            name = bpa_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            bpa_ids = list(sorted(set([t.bpa_id for t in rows if t.bpa_id])))
            obj.update(self.google_drive_track_to_object(track_meta))
            self.apply_common_context(obj, bpa_ids)
            obj.update({
                'name': name,
                'id': name,
                'notes': '%s' % (folder_name),
                'title': '%s' % (folder_name),
                'omics': 'proteomics',
                'bpa_ids': ', '.join(bpa_ids),
                'data_generated': 'True',
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            tag_names = sepsis_contextual_tags(self, obj)
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        # one MD5 file per 'folder_name', so we just take every file and upload
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            with open(md5_file) as fd:
                for md5, filename in md5lines(fd):
                    resource = {}
                    resource['md5'] = resource['id'] = md5
                    resource['name'] = filename
                    xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                    folder_name = self.google_track_meta.get(xlsx_info['ticket']).folder_name
                    legacy_url = urljoin(xlsx_info['base_url'], filename)
                    resources.append(((folder_name,), legacy_url, resource))
        return resources


class SepsisTranscriptomicsAnalysedMetadata(BaseSepsisAnalysedMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsSwathMSContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/transcriptomics/analysed/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata\.xlsx$']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-transcriptomics-analysed'
    resource_linkage = ('folder_name',)
    omics = 'transcriptomics'
    technology = 'analysed'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisTranscriptomicsAnalysedMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata()
        self.bpam_track_meta = [SepsisTrackMetadata('TranscriptomicsHiSeq')]

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
        field_spec = [
            ('data_analysis_date', 'data analysis date (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('bpa_id', 'sample name (5 digit bpa id)', ingest_utils.extract_bpa_id),
            ('taxon_or_organism', 'taxon_or_organism'),
            ('strain_or_isolate', 'strain_or_isolate'),
            ('serovar', 'serovar'),
            ('growth_media', 'growth media'),
            ('replicate', 'replicate', ingest_utils.get_int),
            ('growth_condition_time', 'growth_condition_time'),
            ('growth_condition_growth_phase', 'growth_condition_growth phase'),
            ('growth_condition_od600_reading', 'growth_condition_od600 reading'),
            ('growth_condition_temperature', 'growth_condition_temperature'),
            ('growth_condition_media', 'growth_condition_media'),
            ('omics', 'omics'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('alignment_file_name', 'alignment file name'),
            ('file_name_of_gene_list', 'file name of gene list (raw counts)'),
            ('file_name_of_annotated_rna', 'file name of annotated rna'),
            ('file_name_of_assembled_genome', 'file name of assembled genome used for analysis'),
            ('file_name_of_annotated_genes', 'file name of annotated genes used for analysis'),
            ('approach_used', 'approach used'),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=8,
            column_name_row_index=7,
            formatting_info=True,
            additional_context=additional_context)
        rows = list(wrapper.get_all())
        return rows

    def _get_packages(self):
        logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, xlsx_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode('utf8')).hexdigest()
            name = bpa_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            bpa_ids = list(sorted(set([t.bpa_id for t in rows])))
            obj.update(self.google_drive_track_to_object(track_meta))
            self.apply_common_context(obj, bpa_ids)
            obj.update({
                'name': name,
                'id': name,
                'notes': '%s' % (folder_name),
                'title': '%s' % (folder_name),
                'omics': 'transcriptomics',
                'bpa_ids': ', '.join(bpa_ids),
                'data_generated': 'True',
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            tag_names = sepsis_contextual_tags(self, obj)
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        # one MD5 file per 'folder_name', so we just take every file and upload
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            with open(md5_file) as fd:
                for md5, filename in md5lines(fd):
                    resource = {}
                    resource['md5'] = resource['id'] = md5
                    resource['name'] = filename
                    xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                    folder_name = self.google_track_meta.get(xlsx_info['ticket']).folder_name
                    legacy_url = urljoin(xlsx_info['base_url'], filename)
                    resources.append(((folder_name,), legacy_url, resource))
        return resources


class SepsisMetabolomicsAnalysedMetadata(BaseSepsisAnalysedMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsSwathMSContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/metabolomics/analysed/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata\.xlsx$']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-metabolomics-analysed'
    resource_linkage = ('folder_name',)
    omics = 'metabolomics'
    technology = 'analysed'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisMetabolomicsAnalysedMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata()
        self.bpam_track_meta = [SepsisTrackMetadata('MetabolomicsLCMS')]

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
        field_spec = [
            ('data_analysis_date', 'data analysis date (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('bpa_id', 'sample name (5 digit bpa id)', ingest_utils.extract_bpa_id),
            ('taxon_or_organism', 'taxon_or_organism'),
            ('strain_or_isolate', 'strain_or_isolate'),
            ('serovar', 'serovar'),
            ('growth_media', 'growth media'),
            ('replicate', 'replicate', ingest_utils.get_int),
            ('growth_condition_time', 'growth_condition_time'),
            ('growth_condition_growth_phase', 'growth_condition_growth phase'),
            ('growth_condition_od600_reading', 'growth_condition_od600 reading'),
            ('growth_condition_temperature', 'growth_condition_temperature'),
            ('growth_condition_media', 'growth_condition_media'),
            ('omics', 'omics'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('data_type', 'data type'),
            ('file_name_of_analysed_data', 'file name of analysed data'),
            ('approach_used', 'approach used'),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=8,
            column_name_row_index=7,
            formatting_info=True,
            additional_context=additional_context)
        rows = list(wrapper.get_all())
        return rows

    def _get_packages(self):
        logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, xlsx_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode('utf8')).hexdigest()
            name = bpa_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            bpa_ids = list(sorted(set([t.bpa_id for t in rows])))
            obj.update(self.google_drive_track_to_object(track_meta))
            self.apply_common_context(obj, bpa_ids)
            obj.update({
                'name': name,
                'id': name,
                'notes': '%s' % (folder_name),
                'title': '%s' % (folder_name),
                'omics': 'metabolomics',
                'bpa_ids': ', '.join(bpa_ids),
                'data_generated': 'True',
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            tag_names = sepsis_contextual_tags(self, obj)
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        # one MD5 file per 'folder_name', so we just take every file and upload
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            with open(md5_file) as fd:
                for md5, filename in md5lines(fd):
                    resource = {}
                    resource['md5'] = resource['id'] = md5
                    resource['name'] = filename
                    xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                    folder_name = self.google_track_meta.get(xlsx_info['ticket']).folder_name
                    legacy_url = urljoin(xlsx_info['base_url'], filename)
                    resources.append(((folder_name,), legacy_url, resource))
        return resources


class SepsisGenomicsAnalysedMetadata(BaseSepsisAnalysedMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisProteomicsSwathMSContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/analysed/']
    metadata_url_components = ('facility_code', 'ticket')
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata\.xlsx$']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')
    ckan_data_type = 'arp-genomics-analysed'
    resource_linkage = ('folder_name',)
    omics = 'genomics'
    technology = 'analysed'

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(SepsisGenomicsAnalysedMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.google_track_meta = SepsisGoogleTrackMetadata()
        self.bpam_track_meta = [SepsisGenomicsTrackMetadata('MetabolomicsLCMS')]

    @classmethod
    def parse_spreadsheet(self, fname, additional_context):
        field_spec = [
            ('data_analysis_date', 'data analysis date (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('bpa_id', 'sample name (5 digit bpa id)', ingest_utils.extract_bpa_id),
            ('taxon_or_organism', 'taxon_or_organism'),
            ('strain_or_isolate', 'strain_or_isolate'),
            ('serovar', 'serovar'),
            ('growth_condition_time', 'growth_condition_time'),
            ('growth_condition_temperature', 'growth_condition_temperature'),
            ('growth_condition_media', 'growth_condition_media'),
            ('growth_condition_notes', 'growth_condition_notes'),
            ('experimental_replicate', 'experimental_replicate', ingest_utils.get_int),
            ('analytical_platform', 'analytical_platform'),
            ('analytical_facility', 'analytical_facility'),
            ('experimental_sample_preparation_method', 'experimental_sample_preparation_method'),
            ('data_type', 'data type'),
            ('sample_folder', 'folder for each sample (individual files are listed on the next sheet)'),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=8,
            column_name_row_index=7,
            formatting_info=True,
            additional_context=additional_context)
        rows = list(wrapper.get_all())
        return rows

    def _get_packages(self):
        logger.info("Ingesting Sepsis metadata from {0}".format(self.path))
        # we have one package per Zip of analysed data, and we take the common
        # meta-data for each bpa-id
        folder_rows = defaultdict(list)
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            ticket = xlsx_info['ticket']
            if not ticket:
                continue
            folder_name = self.google_track_meta.get(ticket).folder_name
            for row in self.parse_spreadsheet(fname, xlsx_info):
                folder_rows[(ticket, folder_name)].append(row)
        packages = []
        for (ticket, folder_name), rows in list(folder_rows.items()):
            obj = common_values([t._asdict() for t in rows])
            # we're hitting the 100-char limit, so we have to hash the folder name when
            # generating the CKAN name
            folder_name_md5 = md5hash(folder_name.encode('utf8')).hexdigest()
            name = bpa_id_to_ckan_name(folder_name_md5, self.ckan_data_type)
            track_meta = self.google_track_meta.get(ticket)
            bpa_ids = list(sorted(set([t.bpa_id for t in rows if t.bpa_id])))
            obj.update(self.google_drive_track_to_object(track_meta))
            self.apply_common_context(obj, bpa_ids)
            obj.update({
                'name': name,
                'id': name,
                'notes': '%s' % (folder_name),
                'title': '%s' % (folder_name),
                'omics': 'genomics',
                'bpa_ids': ', '.join(bpa_ids),
                'data_generated': 'True',
                'type': self.ckan_data_type,
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'private': True,
            })
            tag_names = sepsis_contextual_tags(self, obj)
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        # one MD5 file per 'folder_name', so we just take every file and upload
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            with open(md5_file) as fd:
                for md5, filename in md5lines(fd):
                    resource = {}
                    resource['md5'] = resource['id'] = md5
                    resource['name'] = filename
                    xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                    folder_name = self.google_track_meta.get(xlsx_info['ticket']).folder_name
                    legacy_url = urljoin(xlsx_info['base_url'], filename)
                    resources.append(((folder_name,), legacy_url, resource))
        return resources
