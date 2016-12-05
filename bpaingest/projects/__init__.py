from .barcode.ingest import BarcodeMetadata
from .base.ingest import SoilMetadata
from .gbr_amplicons.ingest import GbrAmpliconsMetadata
from .melanoma.ingest import MelanomaMetadata
from .mm.ingest import MarineMicrobesAmpliconsMetadata
from .sepsis.ingest import (
    SepsisGenomicsMiseqMetadata,
    SepsisTranscriptomicsHiseqMetadata,
    SepsisGenomicsPacbioMetadata,
    SepsisMetabolomicsLCMSMetadata,
    SepsisProteomicsMS1QuantificationMetadata,
    SepsisProteomicsSwathMSMetadata,
    SepsisProteomicsSwathMSPoolMetadata)
from .stemcells.ingest import StemcellsMetadata
from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_pathogens_genomes.ingest import WheatPathogensGenomesMetadata
from .wheat_pathogens_transcript.ingest import WheatPathogensTranscriptMetadata

PROJECTS = {
    'barcode': BarcodeMetadata,
    'base': SoilMetadata,
    'gbr': GbrAmpliconsMetadata,
    'melanoma': MelanomaMetadata,
    'mm-amplicons': MarineMicrobesAmpliconsMetadata,
    'sepsis-genomics-miseq': SepsisGenomicsMiseqMetadata,
    'sepsis-genomics-pacbio': SepsisGenomicsPacbioMetadata,
    'sepsis-transcriptomics-hiseq': SepsisTranscriptomicsHiseqMetadata,
    'sepsis-metabolomics-lcms': SepsisMetabolomicsLCMSMetadata,
    'sepsis-proteomics-ms1quantification': SepsisProteomicsMS1QuantificationMetadata,
    'sepsis-proteomics-swathms': SepsisProteomicsSwathMSMetadata,
    'sepsis-proteomics-swathms-pool': SepsisProteomicsSwathMSPoolMetadata,
    'stemcells': StemcellsMetadata,
    'wheat-cultivars': WheatCultivarsMetadata,
    'wheat-pathogens-genomes': WheatPathogensGenomesMetadata,
    'wheat-pathogens-transcript': WheatPathogensTranscriptMetadata,
}
