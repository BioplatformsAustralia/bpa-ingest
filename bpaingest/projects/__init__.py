from .barcode.ingest import BarcodeMetadata
from .base.ingest import (
    BASEAmpliconsMetadata,
    BASEMetagenomicsMetadata)
from .gbr_amplicons.ingest import GbrAmpliconsMetadata
from .melanoma.ingest import MelanomaMetadata
from .mm.ingest import (
    MarineMicrobesGenomicsAmplicons16SMetadata,
    MarineMicrobesGenomicsAmpliconsA16SMetadata,
    MarineMicrobesGenomicsAmplicons18SMetadata,
    MarineMicrobesGenomicsAmplicons16SControlMetadata,
    MarineMicrobesGenomicsAmpliconsA16SControlMetadata,
    MarineMicrobesGenomicsAmplicons18SControlMetadata,
    MarineMicrobesMetagenomicsMetadata,
    MarineMicrobesMetatranscriptomeMetadata)
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
    StemcellsSingleCellRNASeqMetadata,
    StemcellsMetabolomicMetadata,
    StemcellsProteomicMetadata,
    StemcellsAnalysedProteomicMetadata)
from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_pathogens_genomes.ingest import WheatPathogensGenomesMetadata
from .wheat_pathogens_transcript.ingest import WheatPathogensTranscriptMetadata
from .omg.ingest import (OMG10XProcessedIlluminaMetadata, OMG10XRawIlluminaMetadata)

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
    'stemcells-transcriptome': StemcellsTranscriptomeMetadata,
    'stemcells-smallrna': StemcellsSmallRNAMetadata,
    'stemcells-singlecellrnaseq': StemcellsSingleCellRNASeqMetadata,
    'stemcells-metabolomic': StemcellsMetabolomicMetadata,
    'stemcells-proteomic': StemcellsProteomicMetadata,
    'mm-genomics-amplicons-16s': MarineMicrobesGenomicsAmplicons16SMetadata,
    'mm-genomics-amplicons-a16s': MarineMicrobesGenomicsAmpliconsA16SMetadata,
    'mm-genomics-amplicons-18s': MarineMicrobesGenomicsAmplicons18SMetadata,
    'mm-genomics-amplicons-16s-control': MarineMicrobesGenomicsAmplicons16SControlMetadata,
    'mm-genomics-amplicons-a16s-control': MarineMicrobesGenomicsAmpliconsA16SControlMetadata,
    'mm-genomics-amplicons-18s-control': MarineMicrobesGenomicsAmplicons18SControlMetadata,
    'mm-metagenomics': MarineMicrobesMetagenomicsMetadata,
    'mm-metatranscriptome': MarineMicrobesMetatranscriptomeMetadata,
    'base-amplicons': BASEAmpliconsMetadata,
    'base-metagenomics': BASEMetagenomicsMetadata,
    'omg-10xraw': OMG10XRawIlluminaMetadata,
    'omg-10xprocessed': OMG10XProcessedIlluminaMetadata,
    # under development
    'stemcells-analysed-proteomic': StemcellsAnalysedProteomicMetadata,
    # stubs
    'barcode': BarcodeMetadata,
    'melanoma': MelanomaMetadata,
    'wheat-pathogens-transcript': WheatPathogensTranscriptMetadata,
}
