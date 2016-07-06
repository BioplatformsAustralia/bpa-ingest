from ..libs.excel_wrapper import ExcelWrapper
from ..util import make_logger

logger = make_logger(__name__)


def get_cultivar_sample_characteristics(file_name):
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

    wrapper = ExcelWrapper(
        field_spec,
        file_name,
        sheet_name="Characteristics",
        header_length=1)
    return wrapper.get_all()


def parse_sample_data(path):
    def is_metadata(path):
        if path.isfile() and path.ext == '.xlsx':
            return True

    logger.info('Ingesting Wheat Cultivars metadata from {0}'.format(path))
    samples = {}
    for metadata_file in path.walk(filter=is_metadata):
        logger.info('Processing Wheat Cultivars {0}'.format(metadata_file))
        for sample in get_cultivar_sample_characteristics(metadata_file):
            samples[sample.bpa_id] = sample
    return samples
