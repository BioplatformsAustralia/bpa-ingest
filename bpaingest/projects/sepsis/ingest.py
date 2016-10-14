from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata
from ...libs.excel_wrapper import ExcelWrapper

import files

logger = make_logger(__name__)


class SepsisGenomicsMiseqMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/sepsis/genomics/miseq/'
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

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
                name = bpa_id_to_ckan_name(bpa_id)
                obj = {
                    'name': name,
                    'id': bpa_id,
                    'bpa_id': bpa_id,
                    'title': 'Sepsis Genomics Miseq %s' % (bpa_id),
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'analysis_software_version': row.analysis_software_version,
                    'type': 'arp-genomics-miseq',
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
                bpa_id = file_info.get('id')
                legacy_url = bpa_mirror_url('bpa/sepsis/genomics/miseq/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources


class SepsisTranscriptomicsHiseqMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/sepsis/transcriptomics/hiseq/'
    organization = 'bpa-sepsis'
    auth = ('sepsis', 'sepsis')

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

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
            rows = list(SepsisGenomicsHiseqMetadata.parse_spreadsheet(fname))
            for row in rows:
                bpa_id = row.bpa_id
                name = bpa_id_to_ckan_name(bpa_id)
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
                bpa_id = file_info.get('id')
                legacy_url = bpa_mirror_url('bpa/sepsis/transcriptomics/hiseq/' + file_info.filename)
                resources.append((bpa_id, legacy_url, resource))
        return resources