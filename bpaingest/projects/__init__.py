from .barcode.ingest import BarcodeMetadata
from .base.ingest import SoilMetadata
from .gbr_amplicons.ingest import GbrAmpliconsMetadata
from .melanoma.ingest import MelanomaMetadata
from .mm.ingest import MarineMicrobesMetadata
from .sepsis.ingest import SepsisGenomicsMiseqMetadata
from .stemcells.ingest import StemcellsMetadata
from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_pathogens_genomes.ingest import WheatPathogensGenomesMetadata
from .wheat_pathogens_transcript.ingest import WheatPathogensTranscriptMetadata

PROJECTS = {
    'barcode': BarcodeMetadata,
    'base': SoilMetadata,
    'gbr': GbrAmpliconsMetadata,
    'melanoma': MelanomaMetadata,
    'mm': MarineMicrobesMetadata,
    'sepsis': SepsisGenomicsMiseqMetadata,
    'stemcells': StemcellsMetadata,
    'wheat-cultivars': WheatCultivarsMetadata,
    'wheat-pathogens-genomes': WheatPathogensGenomesMetadata,
    'wheat-pathogens-transcript': WheatPathogensTranscriptMetadata,
}
