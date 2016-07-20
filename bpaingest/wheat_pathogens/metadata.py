from __future__ import print_function

from ..libs.excel_wrapper import ExcelWrapper
from ..util import make_logger

logger = make_logger(__name__)


def get_pathogen_sample_data(file_name):
    """
    This is the data from the Characteristics Sheet
    """
    field_spec = [("bpa_id", "BPA ID", lambda s: s.replace("/", ".")),
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
                  ("lane_number", "Lane number", None),
                  ("qc_software", "AGRF DATA QC software (please include version)", None),  # empty
                  ("sequence_filename", "FILE NAME", None),
                  ("sequence_filetype", "file type", None),
                  ("md5_checksum", "MD5 checksum", None),
                  ("file_size", "Size", None),
                  ("analysis_performed", "analysis performed (to date)", None),
                  ("genbank_project", "GenBank Project", None),
                  ("locus_tag", "Locus tag", None),
                  ("genome_analysis", "Genome-Analysis", None),
                  ("metdata_file", "Metadata file", None)]

    wrapper = ExcelWrapper(field_spec, file_name, sheet_name="Metadata", header_length=1, column_name_row_index=0)
    return wrapper.get_all()


def parse_metadata(path):
    def is_metadata(path):
        if path.isfile() and path.ext == '.xlsx':
            return True

    logger.info('Ingesting Wheat Pathogens metadata from {0}'.format(path))
    rows = []
    for metadata_file in path.walk(filter=is_metadata):
        logger.info('Processing Wheat Cultivars {0}'.format(metadata_file))
        for sample in get_pathogen_sample_data(metadata_file):
            rows.append(sample)
    return rows
