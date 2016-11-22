# -*- coding: utf-8 -*-

from __future__ import print_function

from unipath import Path

from ...libs import ingest_utils
from ...util import make_logger, bpa_id_to_ckan_name, csv_to_named_tuple
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...libs.excel_wrapper import ExcelWrapper
from glob import glob

import files

logger = make_logger(__name__)


def one(l):
    assert(len(l) == 1)
    return l[0]


def get_gram_stain(val):
    if val and val is not '':
        val = val.lower()
        if 'positive' in val:
            return 'POS'
        elif 'negative' in val:
            return 'NEG'
    return None


def get_sex(val):
    if val and val is not '':
        val = val.lower()
        # order of these statements is significant
        if 'female' in val:
            return 'F'
        if 'male' in val:
            return 'M'
    return None


def get_strain_or_isolate(val):
    if val and val is not '':
        # convert floats to str
        if isinstance(val, float):
            val = int(val)
        return str(val)
    return None


class SepsisBacterialContextual(object):
    """
    Bacterial sample metadata: used by each of the -omics classes below.
    """

    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/current/bacterial/']
    name = 'sepsis-bacterial'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, track_meta):
        tpl = (track_meta.taxon_or_organism, track_meta.strain_or_isolate)
        if tpl in self.sample_metadata:
            return self.sample_metadata[tpl]
        logger.warning("no contextual metadata available for: %s" % (repr(tpl)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.taxon_or_organism is None or row.strain_or_isolate is None:
                continue
            strain_tuple = (row.taxon_or_organism, row.strain_or_isolate)
            assert(strain_tuple not in sample_metadata)
            sample_metadata[strain_tuple] = row_meta = {}
            for field in row._fields:
                if field != 'taxon_or_organism' and field != 'strain_or_isolate':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('gram_stain', 'Gram_staining_(positive_or_negative)', get_gram_stain),
            ('taxon_or_organism', 'Taxon_OR_organism', None),
            ('strain_or_isolate', 'Strain_OR_isolate', get_strain_or_isolate),
            ('serovar', 'Serovar', None),
            ('key_virulence_genes', 'Key_virulence_genes', None),
            ('isolation_source', 'Isolation_source', None),
            ('strain_description', 'Strain_description', None),
            ('publication_reference', 'Publication_reference', None),
            ('contact_researcher', 'Contact_researcher', None),
            ('culture_collection_id', 'Culture_collection_ID (alternative name[s])', None),
            ('culture_collection_date', 'Culture_collection_date (YYYY-MM-DD)', ingest_utils.get_date_isoformat),
            ('host_location', 'Host_location (state, country)', None),
            ('host_age', 'Host_age', ingest_utils.get_int),
            ('host_dob', 'Host_DOB (YYYY-MM-DD)', ingest_utils.get_date_isoformat),
            ('host_sex', 'Host_sex (F/M)', get_sex),
            ('host_disease_outcome', 'Host_disease_outcome', None),
            ('host_description', 'Host_description', None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name='Sheet1',
            header_length=5,
            column_name_row_index=4,
            formatting_info=True,
            pick_first_sheet=True)
        return wrapper.get_all()


class SepsisGenomicsContextual(object):
    """
    Genomics sample metadata: used by the genomics classes.
    """

    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/current/genomics/']
    name = 'sepsis-genomics'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, track_meta):
        bpa_id = track_meta.five_digit_bpa_id
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no contextual metadata available for: %s" % (repr(bpa_id)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if not row.bpa_id:
                continue
            if row.bpa_id not in sample_metadata:
                logger.warning("duplicate sample metadata row for {}".format(row.bpa_id))
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'taxon_or_organism' and field != 'strain_or_isolate':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('bpa_id', "BPA_sample_ID", ingest_utils.extract_bpa_id),
            ('taxon_or_organism', "Taxon_OR_organism", None),
            ('strain_or_isolate', "Strain_OR_isolate", None),
            ('serovar', "Serovar", None),
            ('growth_condition_time', "Growth_condition_time", None),
            ('growth_condition_temperature', "Growth_condition_temperature", ingest_utils.get_clean_number),
            ('growth_condition_media', "Growth_condition_media", None),
            ('growth_condition_notes', "Growth_condition_notes", None),
            ('experimental_replicate', "Experimental_replicate", None),
            ('analytical_facility', "Analytical_facility", None),
            ('analytical_platform', "Analytical_platform", None),
            ('experimental_sample_preparation_method', "Experimental_sample_preparation_method", None),
            ('data_type', "Data type", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name='Sheet1',
            header_length=5,
            column_name_row_index=4,
            formatting_info=True,
            pick_first_sheet=True)
        return wrapper.get_all()


class SepsisGenomicsMiseqMetadata(BaseMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisGenomicsContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/miseq/']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('SepsisGenomicsMiseqTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    @classmethod
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
            sheet_name="Sheet1",
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            pick_first_sheet=True)
        return wrapper.get_all()

    def get_packages(self):
        logger.info("Ingesting Sepsis Genomics Miseq metadata from {0}".format(self.path))
        packages = []
        # note: the metadata in the package xlsx is quite minimal
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Genomics metadata file {0}".format(fname))
            rows = list(SepsisGenomicsMiseqMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                track_meta = self.track_meta[bpa_id]
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], 'arp-genomics-miseq')
                obj = {
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'notes': 'ARP Genomics Miseq Data: %s %s' % (track_meta.taxon_or_organism, track_meta.strain_or_isolate),
                    'title': 'Sepsis Genomics Miseq %s' % (bpa_id),
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'analysis_software_version': row.analysis_software_version,
                    'data_type': track_meta.data_type,
                    'taxon_or_organism': track_meta.taxon_or_organism,
                    'strain_or_isolate': track_meta.strain_or_isolate,
                    'serovar': track_meta.serovar,
                    'growth_media': track_meta.growth_media,
                    'replicate': track_meta.replicate,
                    'omics': track_meta.omics,
                    'analytical_platform': track_meta.analytical_platform.strip(),
                    'facility': track_meta.facility,
                    'work_order': track_meta.work_order,
                    'contextual_data_submission_date': track_meta.contextual_data_submission_date,
                    'sample_submission_date': track_meta.sample_submission_date,
                    'data_generated': track_meta.data_generated,
                    'archive_ingestion_date': track_meta.archive_ingestion_date,
                    'type': 'arp-genomics-miseq',
                    'private': True,
                }
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(track_meta))
                tag_names = ['miseq', 'genomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(files.miseq_filename_re, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('index', 'lane', 'vendor', 'read', 'flow_cell_id', 'library', 'extraction', 'runsamplenum'))
                resource['seq_size'] = file_info.get('size')
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                legacy_url = bpa_mirror_url('bpa/sepsis/genomics/miseq/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


class SepsisGenomicsPacbioMetadata(BaseMetadata):
    contextual_classes = [SepsisBacterialContextual, SepsisGenomicsContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/pacbio/']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('SepsisGenomicsPacbioTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    @classmethod
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
            sheet_name="Sheet1",
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            pick_first_sheet=True)
        return wrapper.get_all()

    def get_packages(self):
        logger.info("Ingesting Sepsis Genomics Pacbio metadata from {0}".format(self.path))
        packages = []
        # note: the metadata in the package xlsx is quite minimal
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Genomics metadata file {0}".format(fname))
            rows = list(SepsisGenomicsPacbioMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                track_meta = self.track_meta[bpa_id]
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], 'arp-genomics-pacbio')
                obj = {
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'title': 'Sepsis Genomics Pacbio %s' % (bpa_id),
                    'notes': 'ARP Genomics Pacbio Data: %s %s' % (track_meta.taxon_or_organism, track_meta.strain_or_isolate),
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'sequencer_run_id': row.sequencer_run_id,
                    'smrt_cell_id': row.smrt_cell_id,
                    'cell_position': row.cell_position,
                    'rs_version': row.rs_version,
                    'data_type': track_meta.data_type,
                    'taxon_or_organism': track_meta.taxon_or_organism,
                    'strain_or_isolate': track_meta.strain_or_isolate,
                    'serovar': track_meta.serovar,
                    'growth_media': track_meta.growth_media,
                    'replicate': track_meta.replicate,
                    'omics': track_meta.omics,
                    'analytical_platform': track_meta.analytical_platform.strip(),
                    'facility': track_meta.facility,
                    'work_order': track_meta.work_order,
                    'contextual_data_submission_date': track_meta.contextual_data_submission_date,
                    'sample_submission_date': track_meta.sample_submission_date,
                    'data_generated': track_meta.data_generated,
                    'archive_ingestion_date': track_meta.archive_ingestion_date,
                    'type': 'arp-genomics-pacbio',
                    'private': True,
                }
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(track_meta))
                tag_names = ['pacbio', 'genomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(files.pacbio_filename_re, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('run_id', 'vendor', 'data_type', 'machine_data'))
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                legacy_url = bpa_mirror_url('bpa/sepsis/genomics/pacbio/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


class SepsisTranscriptomicsHiseqMetadata(BaseMetadata):
    contextual_classes = [SepsisBacterialContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/transcriptomics/hiseq/']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisGenomicsHiseqTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    @classmethod
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
            sheet_name="Sheet1",
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            pick_first_sheet=True)
        return wrapper.get_all()

    def get_packages(self):
        logger.info("Ingesting Sepsis Transcriptomics Hiseq metadata from {0}".format(self.path))
        packages = []
        # note: the metadata in the package xlsx is quite minimal
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Transcriptomics metadata file {0}".format(fname))
            rows = list(SepsisTranscriptomicsHiseqMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                track_meta = self.track_meta[bpa_id]
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], 'arp-transcriptomics-hiseq')
                obj = {
                    'name': name,
                    'id': bpa_id,
                    'bpa_id': bpa_id,
                    'title': 'ARP Transcriptomics Hiseq %s' % (bpa_id),
                    'notes': 'ARP Transcriptomics Hiseq Data: %s %s' % (track_meta.taxon_or_organism, track_meta.strain_or_isolate),
                    'sample': row.sample,
                    'library_construction_protocol': row.library_construction_protocol,
                    'barcode_tag': row.barcode_tag,
                    'sequencer': row.sequencer,
                    'casava_version': row.casava_version,
                    'data_type': track_meta.data_type,
                    'taxon_or_organism': track_meta.taxon_or_organism,
                    'strain_or_isolate': track_meta.strain_or_isolate,
                    'serovar': track_meta.serovar,
                    'growth_media': track_meta.growth_media,
                    'replicate': track_meta.replicate,
                    'omics': track_meta.omics,
                    'analytical_platform': track_meta.analytical_platform.strip(),
                    'facility': track_meta.facility,
                    'work_order': track_meta.work_order,
                    'contextual_data_submission_date': track_meta.contextual_data_submission_date,
                    'sample_submission_date': track_meta.sample_submission_date,
                    'data_generated': track_meta.data_generated,
                    'archive_ingestion_date': track_meta.archive_ingestion_date,
                    'type': 'arp-transcriptomics-hiseq',
                    'private': True,
                }
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(track_meta))
                tag_names = ['hiseq', 'transcriptomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(files.hiseq_filename_re, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('library', 'vendor', 'flow_cell_id', 'index', 'lane', 'read'))
                resource['seq_size'] = file_info.get('size')
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                legacy_url = bpa_mirror_url('bpa/sepsis/transcriptomics/hiseq/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


class SepsisMetabolomicsLCMSMetadata(BaseMetadata):
    contextual_classes = [SepsisBacterialContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/metabolomics/lcms/']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisMetabolomicsLCMSTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    @classmethod
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
            sheet_name="Sheet1",
            header_length=1,
            column_name_row_index=1,
            formatting_info=True,
            pick_first_sheet=True)
        return wrapper.get_all()

    def get_packages(self):
        packages = []
        # note: the metadata in the package xlsx is quite minimal
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Metabolomics LCMS metadata file {0}".format(fname))
            rows = list(SepsisMetabolomicsLCMSMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                track_meta = self.track_meta[bpa_id]
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], 'arp-metabolomics-lcms')
                obj = {
                    'name': name,
                    'id': bpa_id,
                    'bpa_id': bpa_id,
                    'title': 'ARP Metabolomics LCMS %s' % (bpa_id),
                    'notes': 'ARP Metabolomics LCMS Data: %s %s' % (track_meta.taxon_or_organism, track_meta.strain_or_isolate),
                    'sample_fractionation_extract_solvent': row.sample_fractionation_extract_solvent,
                    'lc_column_type': row.lc_column_type,
                    'gradient_time_min_flow': row.gradient_time_min_flow,
                    'mass_spectrometer': row.mass_spectrometer,
                    'acquisition_mode': row.acquisition_mode,
                    'raw_file_name': row.raw_file_name,
                    'data_type': track_meta.data_type,
                    'taxon_or_organism': track_meta.taxon_or_organism,
                    'strain_or_isolate': track_meta.strain_or_isolate,
                    'serovar': track_meta.serovar,
                    'growth_media': track_meta.growth_media,
                    'replicate': track_meta.replicate,
                    'omics': track_meta.omics,
                    'analytical_platform': track_meta.analytical_platform.strip(),
                    'facility': track_meta.facility,
                    'work_order': track_meta.work_order,
                    'contextual_data_submission_date': track_meta.contextual_data_submission_date,
                    'sample_submission_date': track_meta.sample_submission_date,
                    'data_generated': track_meta.data_generated,
                    'archive_ingestion_date': track_meta.archive_ingestion_date,
                    'archive_id': track_meta.archive_id,
                    'type': 'arp-metabolomics-lcms',
                    'private': True,
                }
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(track_meta))
                tag_names = ['lcms', 'metabolomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(files.metabolomics_deepclms_filename_re, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('vendor', 'platform', 'mastr_ms_id', 'machine_data'))
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                legacy_url = bpa_mirror_url('bpa/sepsis/metabolomics/lcms/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


class SepsisProteomicsMS1QuantificationMetadata(BaseMetadata):
    contextual_classes = [SepsisBacterialContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/ms1quantification/']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisProteomicsMS1QuantificationTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    @classmethod
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
            sheet_name="Sheet1",
            header_length=1,
            column_name_row_index=1,
            formatting_info=True,
            pick_first_sheet=True)
        return wrapper.get_all()

    def get_packages(self):
        packages = []
        # note: the metadata in the package xlsx is quite minimal
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Sepsis Proteomics MS1Quantification metadata file {0}".format(fname))
            rows = list(SepsisProteomicsMS1QuantificationMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                track_meta = self.track_meta[bpa_id]
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], 'arp-proteomics-ms1quantification')
                obj = {
                    'name': name,
                    'id': bpa_id,
                    'bpa_id': bpa_id,
                    'title': 'ARP Proteomics LCMS %s' % (bpa_id),
                    'notes': 'ARP Proteomics LCMS Data: %s %s' % (track_meta.taxon_or_organism, track_meta.strain_or_isolate),
                    'sample_fractionation_none_number': row.sample_fractionation_none_number,
                    'lc_column_type': row.lc_column_type,
                    'gradient_time_per_acn': row.gradient_time_per_acn,
                    'sample_on_column': row.sample_on_column,
                    'mass_spectrometer': row.mass_spectrometer,
                    'acquisition_mode_fragmentation': row.acquisition_mode_fragmentation,
                    'raw_file_name': row.raw_file_name,
                    'data_type': track_meta.data_type,
                    'taxon_or_organism': track_meta.taxon_or_organism,
                    'strain_or_isolate': track_meta.strain_or_isolate,
                    'serovar': track_meta.serovar,
                    'growth_media': track_meta.growth_media,
                    'replicate': track_meta.replicate,
                    'omics': track_meta.omics,
                    'analytical_platform': track_meta.analytical_platform.strip(),
                    'facility': track_meta.facility,
                    'work_order': track_meta.work_order,
                    'contextual_data_submission_date': track_meta.contextual_data_submission_date,
                    'sample_submission_date': track_meta.sample_submission_date,
                    'data_generated': track_meta.data_generated,
                    'archive_ingestion_date': track_meta.archive_ingestion_date,
                    'archive_id': track_meta.archive_id,
                    'type': 'arp-proteomics-ms1quantification',
                    'private': True,
                }
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(track_meta))
                tag_names = ['ms1quantification', 'proteomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(files.proteomics_deepclms_filename_re, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('vendor', 'machine_data'))
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                legacy_url = bpa_mirror_url('bpa/sepsis/proteomics/ms1quantification/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


class SepsisProteomicsSwathMSMetadata(BaseMetadata):
    contextual_classes = [SepsisBacterialContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/swathms/']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisProteomicsSwathMSTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    @classmethod
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
            sheet_name="Sheet1",
            header_length=1,
            column_name_row_index=1,
            formatting_info=True,
            pick_first_sheet=True)
        return wrapper.get_all()

    def get_packages(self):
        packages = []
        # note: the metadata in the package xlsx is quite minimal
        for fname in glob(self.path + '/*_metadata.xlsx'):
            logger.info("Processing Sepsis Proteomics SwathMS metadata file {0}".format(fname))
            rows = list(SepsisProteomicsSwathMSMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                track_meta = self.track_meta[bpa_id]
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], 'arp-proteomics-swathms')
                obj = {
                    'name': name,
                    'id': bpa_id,
                    'bpa_id': bpa_id,
                    'title': 'ARP Proteomics LCMS %s' % (bpa_id),
                    'notes': 'ARP Proteomics LCMS Data: %s %s' % (track_meta.taxon_or_organism, track_meta.strain_or_isolate),
                    'sample_fractionation_none_number': row.sample_fractionation_none_number,
                    'lc_column_type': row.lc_column_type,
                    'gradient_time_per_acn': row.gradient_time_per_acn,
                    'sample_on_column': row.sample_on_column,
                    'mass_spectrometer': row.mass_spectrometer,
                    'acquisition_mode_fragmentation': row.acquisition_mode_fragmentation,
                    'raw_file_name': row.raw_file_name,
                    'data_type': track_meta.data_type,
                    'taxon_or_organism': track_meta.taxon_or_organism,
                    'strain_or_isolate': track_meta.strain_or_isolate,
                    'serovar': track_meta.serovar,
                    'growth_media': track_meta.growth_media,
                    'replicate': track_meta.replicate,
                    'omics': track_meta.omics,
                    'analytical_platform': track_meta.analytical_platform.strip(),
                    'facility': track_meta.facility,
                    'work_order': track_meta.work_order,
                    'contextual_data_submission_date': track_meta.contextual_data_submission_date,
                    'sample_submission_date': track_meta.sample_submission_date,
                    'data_generated': track_meta.data_generated,
                    'archive_ingestion_date': track_meta.archive_ingestion_date,
                    'archive_id': track_meta.archive_id,
                    'type': 'arp-proteomics-swathms',
                    'private': True,
                }
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(track_meta))
                tag_names = ['swathms', 'proteomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(files.proteomics_swathms_filename_re, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('vendor', 'machine_data'))
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                legacy_url = bpa_mirror_url('bpa/sepsis/proteomics/swathms/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources
