from __future__ import print_function

from .ops import update_or_create
from unipath import Path
from .util import make_logger
from .libs.excel_wrapper import ExcelWrapper
from .libs.fetch_data import Fetcher

# all metadata and checksums should be linked out here
METADATA_URL = 'https://downloads-qcif.bioplatforms.com/bpa/wheat_cultivars/tracking/'

logger = make_logger('wheatcultivars')


def make_group(ckan):
    return update_or_create(ckan, 'group', {
        'name': 'wheat-cultivars',
        'title': 'Wheat Cultivars',
        'display_name': 'Wheat Cultivars'
    })


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


def ingest_samples(samples):
    """
    Add all the cultivar samples
    """

    wheat_organism, _ = Organism.objects.get_or_create(genus='Triticum', species='Aestivum')

    def add_sample(e):
        """
        Adds cultivar sample from spreadsheet
        """

        bpa_id = get_bpa_id(e)
        if bpa_id is None:
            return

        cultivar_sample, created = CultivarSample.objects.get_or_create(bpa_id=bpa_id, organism=wheat_organism)

        cultivar_sample.name = e.variety  # DDD
        cultivar_sample.variety = e.variety
        cultivar_sample.cultivar_code = e.code
        cultivar_sample.source_name = e.source_name
        cultivar_sample.characteristics = e.characteristics
        cultivar_sample.organism = wheat_organism

        cultivar_sample.organism_part = e.organism_part
        cultivar_sample.pedigree = e.pedigree
        cultivar_sample.dev_stage = e.dev_stage
        cultivar_sample.yield_properties = e.yield_properties
        cultivar_sample.morphology = e.morphology
        cultivar_sample.maturity = e.maturity

        cultivar_sample.pathogen_tolerance = e.pathogen_tolerance
        cultivar_sample.drought_tolerance = e.drought_tolerance
        cultivar_sample.soil_tolerance = e.soil_tolerance

        cultivar_sample.classification = e.classification
        cultivar_sample.url = e.url

        cultivar_sample.debug_note = ingest_utils.INGEST_NOTE + ingest_utils.pretty_print_namedtuple(e)

        cultivar_sample.save()
        logger.info("Ingested Cultivars sample {0}".format(cultivar_sample.name))

    for sample in samples:
        add_sample(sample)


def do_metadata(path):
    def is_metadata(path):
        if path.isfile() and path.ext == '.xlsx':
            return True

    logger.info('Ingesting Wheat Cultivars metadata from {0}'.format(path))
    for metadata_file in path.walk(filter=is_metadata):
        logger.info('Processing Wheat Cultivars {0}'.format(metadata_file))
        sample_data = list(get_cultivar_sample_characteristics(metadata_file))
        ingest_samples(sample_data)


def download(metadata_path, clean):
    fetcher = Fetcher(metadata_path, METADATA_URL)
    if clean:
        fetcher.clean()
    fetcher.fetch_metadata_from_folder()


def ingest(ckan, metadata_path):
    path = Path(metadata_path)
    group = make_group(ckan)
    do_metadata(path)
