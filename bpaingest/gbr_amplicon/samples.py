from ..util import make_logger

logger = make_logger(__name__)


def sample_from_row(e):
    obj = {
        'bpa_id': e.bpa_id,
        'sample_extraction_id': e.sample_extraction_id,
        'sequencing_facility': e.sequencing_facility,
        'amplicon': e.amplicon,
        'i7_index': e.i7_index,
        'i5_index': e.i5_index,
        'index1': e.index1,
        'index2': e.index2,
        'pcr_1_to_10': e.pcr_1_to_10,
        'pcr_1_to_100': e.pcr_1_to_100,
        'pcr_neat': e.pcr_neat,
        'dilution': e.dilution,
        'sequencing_run_number': e.sequencing_run_number,
        'flow_cell_id': e.flow_cell_id,
        'reads': e.reads,
        'sample_name': e.sample_name,
        'analysis_software_version': e.analysis_software_version,
        'notes': e.comments,
    }
    return obj


def samples_from_metadata(metadata):
    samples = {}
    for row in metadata:
        sample = sample_from_row(row)
        samples[sample['bpa_id']] = sample
    return samples
