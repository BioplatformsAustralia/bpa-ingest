from __future__ import print_function

from unipath import Path

from ...util import make_logger, bpa_id_to_ckan_name
from ...bpa import bpa_mirror_url
from ...abstract import BaseMetadata

import files

logger = make_logger(__name__)


class SepsisGenomicsMiseqMetadata(BaseMetadata):
    metadata_url = 'https://downloads-qcif.bioplatforms.com/bpa/sepsis/tracking/'
    organization = 'bpa-sepsis'

    def __init__(self, metadata_path):
        self.path = Path(metadata_path)

    def get_packages(self):
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