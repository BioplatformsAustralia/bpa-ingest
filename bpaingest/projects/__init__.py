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
from .stemcells.ingest import (
    StemcellsTranscriptomeMetadata,
    StemcellsSmallRNAMetadata,
    StemcellsSingleCellRNASeqMetadata)
from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_pathogens_genomes.ingest import WheatPathogensGenomesMetadata
from .wheat_pathogens_transcript.ingest import WheatPathogensTranscriptMetadata

PROJECTS = {
    # functional ingests
    'gbr-genomics-amplicons': GbrAmpliconsMetadata,
    'sepsis-genomics-miseq': SepsisGenomicsMiseqMetadata,
    'sepsis-genomics-pacbio': SepsisGenomicsPacbioMetadata,
    'sepsis-transcriptomics-hiseq': SepsisTranscriptomicsHiseqMetadata,
    'sepsis-metabolomics-lcms': SepsisMetabolomicsLCMSMetadata,
    'sepsis-proteomics-ms1quantification': SepsisProteomicsMS1QuantificationMetadata,
    'sepsis-proteomics-swathms': SepsisProteomicsSwathMSMetadata,
    'sepsis-proteomics-swathms-pool': SepsisProteomicsSwathMSPoolMetadata,
    'wheat-cultivars': WheatCultivarsMetadata,  # the entire wheat cultivars project
    'wheat-pathogens-genomes': WheatPathogensGenomesMetadata,  # the first half of wheat pathogens
    # under development
    'stemcells-transcriptome': StemcellsTranscriptomeMetadata,
    'stemcells-smallrna': StemcellsSmallRNAMetadata,
    'stemcells-singlecellrnaseq': StemcellsSingleCellRNASeqMetadata,
    # stubs
    'barcode': BarcodeMetadata,
    'base': SoilMetadata,
    'melanoma': MelanomaMetadata,
    'marine-genomics-amplicons': MarineMicrobesAmpliconsMetadata,
    'wheat-pathogens-transcript': WheatPathogensTranscriptMetadata,
}
