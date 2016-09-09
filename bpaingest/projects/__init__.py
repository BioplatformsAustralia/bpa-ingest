from .barcode.ingest import BarcodeMetadata
from .base.ingest import SoilMetadata
from .gbr.ingest import GbrMetadata
from .melanoma.ingest import MelanomaMetadata
from .mm.ingest import MarineMicrobesMetadata
from .sepsis.ingest import SepsisMetadata
from .stemcells.ingest import StemcellsMetadata
from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_pathogens_genomes.ingest import WheatPathogensGenomesMetadata
from .wheat_pathogens_transcript.ingest import WheatPathogensTranscriptMetadata

PROJECTS = {
    'barcode': BarcodeMetadata,
    'base': SoilMetadata,
    'gbr': GbrMetadata,
    'melanoma': MelanomaMetadata,
    'mm': MarineMicrobesMetadata,
    'sepsis': SepsisMetadata,
    'stemcells': StemcellsMetadata,
    'wheat-cultivars': WheatCultivarsMetadata,
    'wheat-pathogens-genomes': WheatPathogensGenomesMetadata,
    'wheat-pathogens-transcript': WheatPathogensTranscriptMetadata,
}
