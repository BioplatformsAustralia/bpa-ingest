

import os
import re

from unipath import Path
from urllib.parse import urljoin
from collections import defaultdict
from glob import glob
from ...libs.excel_wrapper import make_field_definition as fld
from ...libs import ingest_utils

from ...util import make_logger, bpa_id_to_ckan_name, common_values
from ...abstract import BaseMetadata

logger = make_logger(__name__)


class WheatPathogensGenomesMetadata(BaseMetadata):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/wheat_pathogens/metadata/2017-07-07/']
    organization = 'bpa-wheat-pathogens-genomes'
    ckan_data_type = 'wheat-pathogens'
    omics = 'genomics'
    spreadsheet = {
        'fields': [
            fld("bpa_id", "BPA ID", coerce=ingest_utils.extract_bpa_id),
            fld("official_variety", "Isolate name"),
            fld("kingdom", "Kingdom"),
            fld("phylum", "Phylum"),
            fld("species", "Species"),
            fld("sample_id", "Researcher Sample ID"),
            fld("other_id", "Other IDs"),
            fld("original_source_host_species", "Original source host species"),
            fld("collection_date", "Isolate collection date"),
            fld("collection_location", "Isolate collection location"),
            fld("wheat_pathogenicity", "Pathogenicity towards wheat"),
            fld("contact_scientist", "Contact scientist"),
            fld("sample_dna_source", "DNA Source"),
            fld("dna_extraction_protocol", "DNA extraction protocol"),
            fld("library", "Library "),
            fld("library_construction", "Library Construction"),
            fld("library_construction_protocol", "Library construction protocol"),
            fld("sequencer", "Sequencer"),
            fld("sample_label", "Sample (AGRF Labelling)"),
            fld("library_id", "Library ID"),
            fld("index_number", "Index #"),
            fld("index_sequence", "Index"),
            fld("run_number", "Run number"),
            fld("flow_cell_id", "Run #:Flow Cell ID"),
            fld("lane_number", re.compile(r'lane.*number')),
            fld("sequence_filename", "FILE NAME"),
            fld("md5_checksum", "MD5 checksum"),
            fld("file_size", "Size"),
            fld("analysis_performed", "analysis performed (to date)"),
            fld("genbank_project", "GenBank Project"),
            fld("locus_tag", "Locus tag"),
            fld("genome_analysis", "Genome-Analysis"),
            fld("metadata_file", "Metadata file"),
        ],
        'options': {
            'sheet_name': "Metadata",
            'header_length': 1,
            'column_name_row_index': 0,
        }
    }

    def __init__(self, metadata_path, metadata_info=None):
        super(WheatPathogensGenomesMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info

    def _get_packages(self):
        packages = []
        for fname in glob(self.path + '/Wheat_pathogens_genomic_metadata.xlsx'):
            logger.info("Processing Stemcells Transcriptomics metadata file {0}".format(fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            # there are duplicates by BPA ID -- the spreadsheet is per-file data
            # including MD5s. Common values per BPA ID extracted to be package metadata
            by_bpaid = defaultdict(list)
            for row in self.parse_spreadsheet(fname, xlsx_info):
                by_bpaid[row.bpa_id].append(row)
            for bpa_id, rows in list(by_bpaid.items()):
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

    def _get_resources(self):
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
                legacy_url = urljoin(xlsx_info['base_url'], '../../all/' + resource['name'])
                resources.append(((bpa_id,), legacy_url, resource))
        return resources
