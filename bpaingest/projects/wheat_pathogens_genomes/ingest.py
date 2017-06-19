from __future__ import print_function

import os
import re

from unipath import Path
from urlparse import urljoin
from collections import defaultdict
from glob import glob
from ...libs.excel_wrapper import ExcelWrapper
from ...libs import ingest_utils

from ...util import make_logger, bpa_id_to_ckan_name, common_values
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class WheatPathogensGenomesMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/wheat_pathogens/metadata/']
    organization = 'bpa-wheat-pathogens-genomes'
    ckan_data_type = 'wheat-pathogens'

    def __init__(self, metadata_path, metadata_info=None):
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info

    @classmethod
    def parse_spreadsheet(cls, file_name, additional_context):
        """
        This is the data from the Characteristics Sheet
        """
        field_spec = [
            ("bpa_id", "BPA ID", ingest_utils.extract_bpa_id),
            ("official_variety", "Isolate name", None),
            ("kingdom", "Kingdom", None),
            ("phylum", "Phylum", None),
            ("species", "Species", None),
            ("sample_id", "Researcher Sample ID", None),
            ("other_id", "Other IDs", None),
            ("original_source_host_species", "Original source host species", None),
            ("collection_date", "Isolate collection date", None),
            ("collection_location", "Isolate collection location", None),
            ("wheat_pathogenicity", "Pathogenicity towards wheat", None),
            ("contact_scientist", "Contact scientist", None),
            ("sample_dna_source", "DNA Source", None),
            ("dna_extraction_protocol", "DNA extraction protocol", None),
            ("library", "Library ", None),
            ("library_construction", "Library Construction", None),
            ("library_construction_protocol", "Library construction protocol", None),
            ("sequencer", "Sequencer", None),
            ("sample_label", "Sample (AGRF Labelling)", None),
            ("library_id", "Library ID", None),
            ("index_number", "Index #", None),
            ("index_sequence", "Index", None),
            ("run_number", "Run number", None),
            ("flow_cell_id", "Run #:Flow Cell ID", None),
            ("lane_number", re.compile(r'lane.*number'), None),
            ("sequence_filename", "FILE NAME", None),
            ("md5_checksum", "MD5 checksum", None),
            ("file_size", "Size", None),
            ("analysis_performed", "analysis performed (to date)", None),
            ("genbank_project", "GenBank Project", None),
            ("locus_tag", "Locus tag", None),
            ("genome_analysis", "Genome-Analysis", None),
            ("metadata_file", "Metadata file", None)]

        wrapper = ExcelWrapper(field_spec, file_name, sheet_name="Metadata", header_length=1, column_name_row_index=0, additional_context=additional_context)
        return wrapper.get_all()

    def get_packages(self):
        packages = []
        for fname in glob(self.path + '/Wheat_pathogens_genomic_metadata.xlsx'):
            logger.info("Processing Stemcells Transcriptomics metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            # there are duplicates by BPA ID -- the spreadsheet is per-file data
            # including MD5s. Common values per BPA ID extracted to be package metadata
            by_bpaid = defaultdict(list)
            for row in self.parse_spreadsheet(fname, xlsx_info):
                by_bpaid[row.bpa_id].append(row)
            for bpa_id, rows in by_bpaid.items():
                data = common_values([t._asdict() for t in rows])
                bpa_id = data['bpa_id']
                if bpa_id is None:
                    continue
                name = bpa_id_to_ckan_name(bpa_id)
                obj = {
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'title': bpa_id,
                    'notes': '%s' % (data['official_variety']),
                    'type': self.ckan_data_type,
                    'bpa_id': bpa_id,
                    'kingdom': data['kingdom'],
                    'phylum': data['phylum'],
                    'species': data['species'],
                    'sample_id': data['sample_id'],
                    'sample_label': data['other_id'],
                    'dna_source': data['sample_dna_source'],
                    'official_variety_name': data['official_variety'],
                    'original_source_host_species': data['original_source_host_species'],
                    'wheat_pathogenicity': data['wheat_pathogenicity'],
                    'index': data['index_sequence'],
                    'library_id': data['library_id'],
                    'collection_date': ingest_utils.get_date_isoformat(data['collection_date']),
                    'collection_location': data['collection_location'],
                    'dna_extraction_protocol': data['dna_extraction_protocol'],
                    'contact_scientist': data['contact_scientist'],
                    'sequencing_facility': 'AGRF',
                    'private': False,
                }
                packages.append(obj)
            return packages

    def get_resources(self):
        def get_file_name(s):
            return os.path.split(s)[1].strip()

        resources = []
        for fname in glob(self.path + '/Wheat_pathogens_genomic_metadata.xlsx'):
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            for row in self.parse_spreadsheet(fname, xlsx_info):
                bpa_id = row.bpa_id
                resource = {
                    'flowcell': row.flow_cell_id,
                    'run_number': ingest_utils.get_clean_number(row.run_number),
                    'sequencer': row.sequencer or "Unknown",
                    'run_index_number': row.index_number,
                    'run_lane_number': ingest_utils.get_clean_number(row.lane_number) or 'none',
                    'run_protocol': row.library_construction_protocol,
                    'run_protocol_base_pairs': ingest_utils.get_clean_number(row.library_construction),
                    'run_protocol_library_type': row.library,
                    'index_number': ingest_utils.get_clean_number(row.index_number),
                    'lane_number': ingest_utils.get_clean_number(row.lane_number),
                    'name': get_file_name(row.sequence_filename),
                    'file_size': row.file_size,
                    'resource_type': self.ckan_data_type,
                }
                resource['md5'] = resource['id'] = row.md5_checksum
                legacy_url = urljoin(xlsx_info['base_url'], '../all/' + resource['name'])
                resources.append(((bpa_id,), legacy_url, resource))
        return resources
