from .base.ingest import (
    BASEAmpliconsMetadata,
    BASEMetagenomicsMetadata,
    BASEAmpliconsControlMetadata,
    BASESiteImagesMetadata)
from .gbr.ingest import GbrAmpliconsMetadata
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
    SepsisProteomicsProteinDatabaseMetadata,
    SepsisProteomicsSwathMSPoolMetadata,
    SepsisProteomicsSwathMSCombinedSampleMetadata,
    SepsisProteomicsAnalysedMetadata,
    SepsisTranscriptomicsAnalysedMetadata,
    SepsisMetabolomicsAnalysedMetadata,
    SepsisGenomicsAnalysedMetadata)
from .stemcells.ingest import (
    StemcellsTranscriptomeMetadata,
    StemcellsSmallRNAMetadata,
    StemcellsSingleCellRNASeqMetadata,
    StemcellsMetabolomicsMetadata,
    StemcellsProteomicsMetadata,
    StemcellsProteomicsPoolMetadata,
    StemcellsProteomicsAnalysedMetadata,
    StemcellsMetabolomicsAnalysedMetadata)
from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_pathogens_genomes.ingest import WheatPathogensGenomesMetadata
from .omg.ingest import (
    OMG10XProcessedIlluminaMetadata,
    OMG10XRawIlluminaMetadata,
    OMG10XRawMetadata,
    OMGExonCaptureMetadata,
    OMGGenomicsHiSeqMetadata)

# from .barcode.ingest import BarcodeMetadata
# from .melanoma.ingest import MelanomaMetadata
# from .wheat_pathogens_transcript.ingest import WheatPathogensTranscriptMetadata


PROJECTS = {
    'gbr-genomics-amplicons': GbrAmpliconsMetadata,
    'sepsis-genomics-miseq': SepsisGenomicsMiseqMetadata,
    'sepsis-genomics-pacbio': SepsisGenomicsPacbioMetadata,
    'sepsis-genomics-analysed': SepsisGenomicsAnalysedMetadata,
    'sepsis-transcriptomics-analysed': SepsisTranscriptomicsAnalysedMetadata,
    'sepsis-transcriptomics-hiseq': SepsisTranscriptomicsHiseqMetadata,
    'sepsis-metabolomics-lcms': SepsisMetabolomicsLCMSMetadata,
    'sepsis-metabolomics-analysed': SepsisMetabolomicsAnalysedMetadata,
    'sepsis-proteomics-ms1quantification': SepsisProteomicsMS1QuantificationMetadata,
    'sepsis-proteomics-swathms': SepsisProteomicsSwathMSMetadata,
    'sepsis-proteomics-swathms-combined-sample': SepsisProteomicsSwathMSCombinedSampleMetadata,
    'sepsis-proteomics-swathms-pool': SepsisProteomicsSwathMSPoolMetadata,
    'sepsis-proteomics-analysed': SepsisProteomicsAnalysedMetadata,
    'sepsis-proteomics-proteindatabase': SepsisProteomicsProteinDatabaseMetadata,
    'wheat-cultivars': WheatCultivarsMetadata,  # the entire wheat cultivars project
    'wheat-pathogens-genomes': WheatPathogensGenomesMetadata,  # the first half of wheat pathogens
    'stemcells-transcriptome': StemcellsTranscriptomeMetadata,
    'stemcells-smallrna': StemcellsSmallRNAMetadata,
    'stemcells-singlecellrnaseq': StemcellsSingleCellRNASeqMetadata,
    'stemcells-metabolomics': StemcellsMetabolomicsMetadata,
    'stemcells-proteomics': StemcellsProteomicsMetadata,
    'stemcells-proteomics-pool': StemcellsProteomicsPoolMetadata,
    'stemcells-proteomics-analysed': StemcellsProteomicsAnalysedMetadata,
    'stemcells-metabolomics-analysed': StemcellsMetabolomicsAnalysedMetadata,
    'mm-genomics-amplicons-16s': MarineMicrobesGenomicsAmplicons16SMetadata,
    'mm-genomics-amplicons-a16s': MarineMicrobesGenomicsAmpliconsA16SMetadata,
    'mm-genomics-amplicons-18s': MarineMicrobesGenomicsAmplicons18SMetadata,
    'mm-genomics-amplicons-16s-control': MarineMicrobesGenomicsAmplicons16SControlMetadata,
    'mm-genomics-amplicons-a16s-control': MarineMicrobesGenomicsAmpliconsA16SControlMetadata,
    'mm-genomics-amplicons-18s-control': MarineMicrobesGenomicsAmplicons18SControlMetadata,
    'mm-metagenomics': MarineMicrobesMetagenomicsMetadata,
    'mm-metatranscriptome': MarineMicrobesMetatranscriptomeMetadata,
    'base-amplicons': BASEAmpliconsMetadata,
    'base-amplicons-control': BASEAmpliconsControlMetadata,
    'base-metagenomics': BASEMetagenomicsMetadata,
    'base-site-images': BASESiteImagesMetadata,
    'omg-10xraw-agrf': OMG10XRawIlluminaMetadata,
    'omg-10xraw': OMG10XRawMetadata,
    'omg-10xprocessed': OMG10XProcessedIlluminaMetadata,
    'omg-exoncapture': OMGExonCaptureMetadata,
    'omg-genomics-hiseq': OMGGenomicsHiSeqMetadata,
    # stubs
    # 'barcode': BarcodeMetadata,
    # 'melanoma': MelanomaMetadata,
    # 'wheat-pathogens-transcript': WheatPathogensTranscriptMetadata,
}
