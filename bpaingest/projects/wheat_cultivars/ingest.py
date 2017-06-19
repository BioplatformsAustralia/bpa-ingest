from __future__ import print_function

import os
from unipath import Path
from glob import glob
from urlparse import urljoin

from ...libs.excel_wrapper import ExcelWrapper
from ...libs import ingest_utils
from ...util import make_logger, bpa_id_to_ckan_name
from ...abstract import BaseMetadata
from ...util import clean_tag_name
from . import files
from .runs import parse_run_data, BLANK_RUN

logger = make_logger(__name__)


class WheatCultivarsMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/wheat_cultivars/tracking/']
    organization = 'bpa-wheat-cultivars'
    ckan_data_type = 'wheat-cultivars'

    def __init__(self, metadata_path, metadata_info=None):
        super(WheatCultivarsMetadata, self).__init__()
        self.metadata_info = metadata_info
        self.path = Path(metadata_path)
        self.runs = parse_run_data(self.path)

    @classmethod
    def parse_spreadsheet(cls, file_name, additional_context):
        """
        This is the data from the Characteristics Sheet
        """

        field_spec = [
            ("source_name", "BPA ID", None),
            ("code", "CODE", None),
            ("bpa_id", "BPA ID", lambda s: s.replace("/", ".")),
            ("characteristics", "Characteristics", None),
            ("organism", "Organism", None),
            ("variety", "Variety", None),
            ("organism_part", "Organism part", None),
            ("pedigree", "Pedigree", None),
            ("dev_stage", "Developmental stage", None),
            ("yield_properties", "Yield properties", None),
            ("morphology", "Morphology", None),
            ("maturity", "Maturity", None),
            ("pathogen_tolerance", "Pathogen tolerance", None),
            ("drought_tolerance", "Drought tolerance", None),
            ("soil_tolerance", "Soil tolerance", None),
            ("classification", "International classification", None),
            ("url", "Link", None),
        ]

        wrapper = ExcelWrapper(field_spec, file_name, sheet_name="Characteristics", header_length=1)
        return wrapper.get_all()

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing Stemcells Transcriptomics metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            for row in self.parse_spreadsheet(fname, xlsx_info):
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                name = bpa_id_to_ckan_name(bpa_id)
                obj = {
                    'name': name,
                    'id': bpa_id,
                    'bpa_id': bpa_id,
                    'title': bpa_id,
                    'notes': '%s (%s): %s' % (row.variety, row.code, row.classification),
                    'type': self.ckan_data_type,
                    'private': False,
                }
                obj.update(dict((t, getattr(row, t)) for t in (
                    'source_name', 'code', 'characteristics', 'classification', 'organism', 'variety',
                    'organism_part', 'pedigree', 'dev_stage', 'yield_properties', 'morphology', 'maturity',
                    'pathogen_tolerance', 'drought_tolerance', 'soil_tolerance', 'url')))
                tag_names = []
                if obj['organism']:
                    tag_names.append(clean_tag_name(obj['organism']))
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource.update(self.runs.get(resource['run'], BLANK_RUN))
                bpa_id = ingest_utils.extract_bpa_id(file_info['bpa_id'])
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_id, ), legacy_url, resource))
        return resources
