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
        for fname in self.path.walk(filter=is_metadata):
            logger.info("Processing Sepsis Genomics metadata file {0}".format(fname))
            rows = list(SepsisGenomicsMiseqMetadata.parse_spreadsheet(fname))
            for row in rows:
                print(row)
        return []

    def get_resources(self):
        def is_md5file(path):
            if path.isfile() and path.ext == ".md5":
                return True

        logger.info("Ingesting Sepsis md5 file information from {0}".format(DATA_DIR))
        for md5_file in DATA_DIR.walk(filter=is_md5file):
            logger.info("Processing Sepsis Genomic md5 file {0}".format(md5_file))
            data = files.parse_md5_file(files.miseq_filename_re, md5_file)
            add_md5(data)
        return []