from ..util import make_logger

logger = make_logger(__name__)


def sample_from_row(e):
    obj = {
        'bpa_id': e.bpa_id,
        'sample_extraction_id': e.sample_extraction_id,
        'sequence_facility': e.sequencing_facility,
        'amplicon': e.amplicon,
        'pcr_1_to_10': e.pcr_1_to_10,
        'pcr_1_to_100': e.pcr_1_to_100,
        'pcr_neat': e.pcr_neat,
        'dilution': e.dilution,
        'name': e.name,
    }
    return obj


def samples_from_metadata(metadata):
    samples = {}
    for row in metadata:
        sample = sample_from_row(row)
        samples[sample['bpa_id']] = sample
    return samples
