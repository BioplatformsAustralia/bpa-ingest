from ..util import make_logger

logger = make_logger(__name__)


def sample_from_row(e):
    obj = {
        'bpa_id': e.bpa_id,
        'kingdom': e.kingdom,
        'phylum': e.phylum,
        'species': e.species,
        'name': e.sample_id,
        'sample_label': e.other_id,
        'dna_source': e.sample_dna_source,
        'official_variety_name': e.official_variety,
        'original_source_host_species': e.original_source_host_species,
        'wheat_pathogenicity': e.wheat_pathogenicity,
        'index': e.index_sequence,
        'library_id': e.library_id,
        'collection_date': e.collection_date,
        'collection_location': e.collection_location,
        'dna_extraction_protocol': e.dna_extraction_protocol,
        'sequencing_facility': "AGRF"
    }
    return obj


def samples_from_metadata(metadata):
    samples = {}
    for row in metadata:
        sample = sample_from_row(row)
        samples[sample['bpa_id']] = sample
    return samples
