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
import os
import re

logger = make_logger(__name__)


bpa_id_re = re.compile(r'^102\.100\.100/(\d+)$')
bpa_id_abbrev_re = re.compile(r'^(\d+)$')


# ignore junk example lines, if been left in by the facility
def extract_bpa_id(s):
    if isinstance(s, float):
        s = int(s)
    if isinstance(s, int):
        s = str(s)
    m = bpa_id_re.match(s)
    if m:
        return m.groups()[0]
    m = bpa_id_abbrev_re.match(s)
    if m:
        return m.groups()[0]
    logger.warning("unable to parse BPA ID: %s" % s)
    return None


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


def prune_blanks(d):
    "remove any empty strings or None values in dictionary keys"
    return dict((k, v) for (k, v) in d.items() if v)


class SepsisBacterialContextual(object):
    """
    Bacterial sample metadata: used by each of the -omics classes below.
    """

    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/']
    name = 'sepsis-bacterial'

    def __init__(self, path):
        xlsx_path = os.path.join(path, 'sepsis_contextual_2016_08_16.xlsx')
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, tpl):
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
            sample_metadata[strain_tuple] = {
                'strain_description': row.strain_description,
                'serovar': row.serovar,
                'key_virulence_genes': row.key_virulence_genes,
                'gram_stain': row.gram_stain,
                'isolation_source': row.isolation_source,
                'publication_reference': row.publication_reference,
                'contact_researcher': row.contact_researcher,
                'culture_collection_date': row.culture_collection_date,
                'study_title': row.study_title,
                'investigation_type': row.investigation_type,
                'project_name': row.project_name,
                'sample_title': row.sample_title or '',
                'ploidy': row.ploidy,
                'num_replicons': row.num_replicons,
                'estimated_size': row.estimated_size,
                'propagation': row.propagation,
                'isolate_growth_condition': row.isolate_growth_condition,
                'collected_by': row.collected_by,
                'growth_condition_time': row.growth_condition_time,
                'growth_condition_temperature': row.growth_condition_temperature,
                'growth_condition_media': row.growth_condition_media,
                'host_description': row.host_description,
                'host_location': row.host_location,
                'host_sex': row.host_sex,
                'host_age': row.host_age,
                'host_dob': row.host_dob,
                'host_disease_outcome': row.host_disease_outcome,
                'host_disease_status': row.host_disease_status,
                'host_associated': row.host_associated,
                'host_health_state': row.host_health_state,
            }
        # we have many metadata sources, avoid them overwriting blank values on each other
        return prune_blanks(sample_metadata)

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('bpa_id', 'BPA_sample_ID', extract_bpa_id),
            ('gram_stain', 'Gram_staining_(positive_or_negative)', get_gram_stain),
            ('taxon_or_organism', 'Taxon_OR_organism', None),
            ('strain_or_isolate', 'Strain_OR_isolate', get_strain_or_isolate),
            ('serovar', 'Serovar', None),
            ('key_virulence_genes', 'Key_virulence_genes', None),
            ('strain_description', 'Strain_description', None),
            ('publication_reference', 'Publication_reference', None),
            ('contact_researcher', 'Contact_researcher', None),
            ('growth_condition_time', 'Growth_condition_time', None),
            ('growth_condition_temperature', 'Growth_condition_temperature', ingest_utils.get_clean_number),
            ('growth_condition_media', 'Growth_condition_media', None),
            ('experimental_replicate', 'Experimental_replicate', None),
            ('analytical_facility', 'Analytical_facility', None),
            ('analytical_platform', 'Analytical_platform', None),
            ('experimental_sample_preparation_method', 'Experimental_sample_preparation_method', None),
            ('culture_collection_id', 'Culture_collection_ID (alternative name[s])', None),
            ('culture_collection_date', 'Culture_collection_date (YYYY-MM-DD)', ingest_utils.get_date_isoformat),
            ('host_location', 'Host_location (state, country)', None),
            ('host_age', 'Host_age', ingest_utils.get_int),
            ('host_dob', 'Host_DOB (DD/MM/YY)', ingest_utils.get_date_isoformat),
            ('host_sex', 'Host_sex (F/M)', get_sex),
            ('host_disease_outcome', 'Host_disease_outcome', None),
            ('isolation_source', 'Isolation_source', None),
            ('host_description', 'Host_description', None),
            ('study_title', 'Study title', None),
            ('investigation_type', 'Investigation_type', None),
            ('project_name', 'Project_name', None),
            ('sample_title', 'Sample title', None),
            ('ploidy', 'ploidy', None),
            ('num_replicons', 'num_replicons', None),
            ('estimated_size', 'estimated_size', None),
            ('propagation', 'propagation', None),
            ('isolate_growth_condition', 'isol_growth_condt', None),
            ('collected_by', 'Collected by', None),
            ('host_associated', 'Host_associated', None),
            ('host_health_state', 'Host_health_state', None),
            ('host_disease_status', 'Host_disease_status', None),
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
    contextual_classes = [SepsisBacterialContextual]
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
        return dict((t.five_digit_bpa_id.split('.')[-1], t) for t in rows)

    @classmethod
    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", lambda s: str(int(s))),
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
                name = bpa_id_to_ckan_name(bpa_id, 'arp-genomics-miseq')
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
                    'analytical_platform': track_meta.analytical_platform,
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
                    obj.update(contextual_source.get((track_meta.taxon_or_organism, track_meta.strain_or_isolate)))
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
                bpa_id = file_info.get('id')
                legacy_url = bpa_mirror_url('bpa/sepsis/genomics/miseq/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


class SepsisGenomicsPacbioMetadata(BaseMetadata):
    contextual_classes = [SepsisBacterialContextual]
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
        return dict((t.five_digit_bpa_id.split('.')[-1], t) for t in rows)

    @classmethod
    def parse_spreadsheet(self, fname):
        def get_bpa_id(val):
            if not val or val is "":
                return None
            return val.replace("102.100.100/", "")
        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", get_bpa_id),
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
                name = bpa_id_to_ckan_name(bpa_id, 'arp-genomics-pacbio')
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
                    'analytical_platform': track_meta.analytical_platform,
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
                    obj.update(contextual_source.get((track_meta.taxon_or_organism, track_meta.strain_or_isolate)))
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
                bpa_id = file_info.get('id')
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
        return dict((t.five_digit_bpa_id.split('.')[-1], t) for t in rows)

    @classmethod
    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", "Antibiotic Resistant Pathogen sample unique ID", extract_bpa_id),
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
                name = bpa_id_to_ckan_name(bpa_id, 'arp-transcriptomics-hiseq')
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
                    'analytical_platform': track_meta.analytical_platform,
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
                    obj.update(contextual_source.get((track_meta.taxon_or_organism, track_meta.strain_or_isolate)))
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
                bpa_id = file_info.get('id')
                legacy_url = bpa_mirror_url('bpa/sepsis/transcriptomics/hiseq/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


class SepsisMetabolomicsDeepLCMSMetadata(BaseMetadata):
    contextual_classes = [SepsisBacterialContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/metabolomics/deeplcms/']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisMetabolomicsDeepLCMSTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((t.five_digit_bpa_id.split('.')[-1], t) for t in rows)

    @classmethod
    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", extract_bpa_id),
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
            logger.info("Processing Sepsis Metabolomics DeepLCMS metadata file {0}".format(fname))
            rows = list(SepsisMetabolomicsDeepLCMSMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                track_meta = self.track_meta[bpa_id]
                name = bpa_id_to_ckan_name(bpa_id, 'arp-metabolomics-deeplcms')
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
                    'analytical_platform': track_meta.analytical_platform,
                    'facility': track_meta.facility,
                    'work_order': track_meta.work_order,
                    'contextual_data_submission_date': track_meta.contextual_data_submission_date,
                    'sample_submission_date': track_meta.sample_submission_date,
                    'data_generated': track_meta.data_generated,
                    'archive_ingestion_date': track_meta.archive_ingestion_date,
                    'archive_id': track_meta.archive_id,
                    'type': 'arp-metabolomics-deeplcms',
                    'private': True,
                }
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get((track_meta.taxon_or_organism, track_meta.strain_or_isolate)))
                tag_names = ['deeplcms', 'metabolomics']
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
                bpa_id = file_info.get('id')
                legacy_url = bpa_mirror_url('bpa/sepsis/metabolomics/deeplcms/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


class SepsisProteomicsDeepLCMSMetadata(BaseMetadata):
    contextual_classes = [SepsisBacterialContextual]
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/proteomics/deeplcms/']
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        if fname is None:
            return {}
        header, rows = csv_to_named_tuple('SepsisProteomicsDeepLCMSTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((t.five_digit_bpa_id.split('.')[-1], t) for t in rows)

    @classmethod
    def parse_spreadsheet(self, fname):

        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", extract_bpa_id),
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
            logger.info("Processing Sepsis Proteomics DeepLCMS metadata file {0}".format(fname))
            rows = list(SepsisProteomicsDeepLCMSMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                track_meta = self.track_meta[bpa_id]
                name = bpa_id_to_ckan_name(bpa_id, 'arp-proteomics-deeplcms')
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
                    'analytical_platform': track_meta.analytical_platform,
                    'facility': track_meta.facility,
                    'work_order': track_meta.work_order,
                    'contextual_data_submission_date': track_meta.contextual_data_submission_date,
                    'sample_submission_date': track_meta.sample_submission_date,
                    'data_generated': track_meta.data_generated,
                    'archive_ingestion_date': track_meta.archive_ingestion_date,
                    'archive_id': track_meta.archive_id,
                    'type': 'arp-proteomics-deeplcms',
                    'private': True,
                }
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get((track_meta.taxon_or_organism, track_meta.strain_or_isolate)))
                tag_names = ['deeplcms', 'proteomics']
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
                bpa_id = file_info.get('id')
                legacy_url = bpa_mirror_url('bpa/sepsis/proteomics/deeplcms/' + file_info.filename)
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
        return dict((t.five_digit_bpa_id.split('.')[-1], t) for t in rows)

    @classmethod
    def parse_spreadsheet(self, fname):

        field_spec = [
            ("bpa_id", "Bacterial sample unique ID", extract_bpa_id),
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
                name = bpa_id_to_ckan_name(bpa_id, 'arp-proteomics-swathms')
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
                    'analytical_platform': track_meta.analytical_platform,
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
                    obj.update(contextual_source.get((track_meta.taxon_or_organism, track_meta.strain_or_isolate)))
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
                bpa_id = file_info.get('id')
                legacy_url = bpa_mirror_url('bpa/sepsis/proteomics/swathms/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources
