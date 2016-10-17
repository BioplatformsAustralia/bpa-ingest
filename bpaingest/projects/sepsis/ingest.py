from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name, csv_to_named_tuple
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...libs.excel_wrapper import ExcelWrapper

import files

logger = make_logger(__name__)


class SepsisGenomicsMiseqMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/miseq/'
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, track_csv_path=None):
        self.path = Path(metadata_path)
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
        def is_metadata(path):
            if path.isfile() and path.ext == ".xlsx":
                return True

        logger.info("Ingesting Sepsis Genomics Miseq metadata from {0}".format(self.path))
        packages = []
        # note: the metadata in the package xlsx is quite minimal
        for fname in self.path.walk(filter=is_metadata):
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
                    'title': 'Sepsis Genomics Miseq %s' % (bpa_id),
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'analysis_software_version': row.analysis_software_version,
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
                tag_names = ['miseq', 'genomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        def is_md5file(path):
            if path.isfile() and path.ext == ".md5":
                return True

        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in self.path.walk(filter=is_md5file):
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
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/pacbio/'
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, track_csv_path=None):
        self.path = Path(metadata_path)
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
        def is_metadata(path):
            if path.isfile() and path.ext == ".xlsx":
                return True

        logger.info("Ingesting Sepsis Genomics Pacbio metadata from {0}".format(self.path))
        packages = []
        # note: the metadata in the package xlsx is quite minimal
        for fname in self.path.walk(filter=is_metadata):
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
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'sequencer_run_id': row.sequencer_run_id,
                    'smrt_cell_id': row.smrt_cell_id,
                    'cell_position': row.cell_position,
                    'rs_version': row.rs_version,
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
                tag_names = ['pacbio', 'genomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        def is_md5file(path):
            if path.isfile() and path.ext == ".md5":
                return True

        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in self.path.walk(filter=is_md5file):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(files.pacbio_filename_re, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('run_id', 'vendor', 'data_type', 'machine_data'))
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                bpa_id = file_info.get('id')
                legacy_url = bpa_mirror_url('bpa/sepsis/genomics/pacbio/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


# NB: as yet there is no HiSeq data, so the below class is untested
class SepsisTranscriptomicsHiseqMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/sepsis/transcriptomics/hiseq/'
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path, track_csv_path=None):
        self.path = Path(metadata_path)
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('SepsisGenomicsHiseqTrack', fname)
        logger.info("track csv header: %s" % (repr(header)))
        return dict((t.five_digit_bpa_id.split('.')[-1], t) for t in rows)

    @classmethod
    def parse_spreadsheet(self, fname):
        field_spec = [
            ("bpa_id", "Antibiotic Resistant Pathogen sample unique ID", lambda s: str(int(s.split('/')[-1]))),
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
        def is_metadata(path):
            if path.isfile() and path.ext == ".xlsx":
                return True

        logger.info("Ingesting Sepsis Transcriptomics Hiseq metadata from {0}".format(self.path))
        packages = []
        # note: the metadata in the package xlsx is quite minimal
        for fname in self.path.walk(filter=is_metadata):
            logger.info("Processing Sepsis Transcriptomics metadata file {0}".format(fname))
            rows = list(SepsisTranscriptomicsHiseqMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                track_meta = self.track_meta[bpa_id]
                name = bpa_id_to_ckan_name(bpa_id, 'arp-transcriptomics-hiseq')
                obj = {
                    'name': name,
                    'id': bpa_id,
                    'bpa_id': bpa_id,
                    'title': 'Sepsis Transcriptomics Hiseq %s' % (bpa_id),
                    'sample': row.sample,
                    'library_construction_protocol': row.library_construction_protocol,
                    'barcode_tag': row.barcode_tag,
                    'sequencer': row.sequencer,
                    'casava_version': row.casava_version,
                    'type': 'arp-genomics-hiseq',
                    'private': True,
                }
                tag_names = ['hiseq', 'transcriptomics']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        def is_md5file(path):
            if path.isfile() and path.ext == ".md5":
                return True

        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in self.path.walk(filter=is_md5file):
            logger.info("Processing md5 file {0}".format(md5_file))
            for file_info in files.parse_md5_file(files.hiseq_filename_re, md5_file):
                resource = dict((t, file_info.get(t)) for t in ('index', 'lane', 'vendor', 'read', 'flow_cell_id', 'library', 'extraction', 'runsamplenum'))
                resource['seq_size'] = file_info.get('size')
                resource['md5'] = resource['id'] = file_info.md5
                resource['name'] = file_info.filename
                bpa_id = file_info.get('id')
                legacy_url = bpa_mirror_url('bpa/sepsis/transcriptomics/hiseq/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources
