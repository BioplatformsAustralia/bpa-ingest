import logging

from .amdb.ingest import (
    AustralianMicrobiomeMetagenomicsAnalysedMetadata,
    AustralianMicrobiomeMetagenomicsNovaseqMetadata,
    AustralianMicrobiomeMetagenomicsNovaseqControlMetadata,
    AustralianMicrobiomeAmpliconsMetadata,
    AustralianMicrobiomeAmpliconsControlMetadata,
    BASEAmpliconsMetadata,
    BASEMetagenomicsMetadata,
    BASEAmpliconsControlMetadata,
    BASESiteImagesMetadata,
    MarineMicrobesAmpliconsMetadata,
    MarineMicrobesAmpliconsControlMetadata,
    MarineMicrobesMetagenomicsMetadata,
    MarineMicrobesMetatranscriptomeMetadata,
)

from .ausarg.ingest import (
    AusargIlluminaFastqMetadata,
    AusargPacbioHifiMetadata,
    AusargONTPromethionMetadata,
    AusargExonCaptureMetadata,
    AusargHiCMetadata,
    AusargGenomicsDArTMetadata,
    AusargGenomicsDDRADMetadata,
)

from .gbr.ingest import GbrAmpliconsMetadata, GbrPacbioMetadata

from .sepsis.ingest import (
    SepsisGenomicsMiseqMetadata,
    SepsisTranscriptomicsHiseqMetadata,
    SepsisGenomicsPacbioMetadata,
    SepsisMetabolomicsLCMSMetadata,
    SepsisMetabolomicsGCMSMetadata,
    SepsisProteomicsMS1QuantificationMetadata,
    SepsisProteomicsSwathMSMetadata,
    SepsisProteomicsProteinDatabaseMetadata,
    SepsisProteomicsSwathMSPoolMetadata,
    SepsisProteomicsSwathMSCombinedSampleMetadata,
    SepsisProteomics2DLibraryMetadata,
    SepsisProteomicsAnalysedMetadata,
    SepsisTranscriptomicsAnalysedMetadata,
    SepsisMetabolomicsAnalysedMetadata,
    SepsisGenomicsAnalysedMetadata,
)
from .stemcells.ingest import (
    StemcellsTranscriptomeMetadata,
    StemcellsSmallRNAMetadata,
    StemcellsSingleCellRNASeqMetadata,
    StemcellsMetabolomicsMetadata,
    StemcellsProteomicsMetadata,
    StemcellsProteomicsPoolMetadata,
    StemcellsProteomicsAnalysedMetadata,
    StemcellsMetabolomicsAnalysedMetadata,
    StemcellsTranscriptomeAnalysedMetadata,
)
from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_pathogens_genomes.ingest import WheatPathogensGenomesMetadata
from .gap.ingest import (
    GAPIlluminaShortreadMetadata,
    GAPONTPromethionMetadata,
    GAPONTMinionMetadata,
    GAPGenomics10XMetadata,
    GAPHiCMetadata,
    GAPGenomicsDDRADMetadata,
    GAPPacbioHifiMetadata,
)
from .omg.ingest import (
    OMG10XProcessedIlluminaMetadata,
    OMG10XRawIlluminaMetadata,
    OMG10XRawMetadata,
    OMGExonCaptureMetadata,
    OMGWholeGenomeMetadata,
    OMGGenomicsNovaseqMetadata,
    OMGGenomicsHiSeqMetadata,
    OMGGenomicsDDRADMetadata,
    OMGGenomicsPacbioMetadata,
    OMGONTPromethionMetadata,
    OMGTranscriptomicsNextseq,
    OMGGenomicsPacBioGenomeAssemblyMetadata,
    OMGAnalysedDataMetadata,
    OMGGenomicsDArTMetadata,
)

from .tsi.ingest import (
    TSIPacbioHifiMetadata,
    TSIGenomicsDDRADMetadata,
    TSIIlluminaShortreadMetadata,
    TSIIlluminaFastqMetadata,
    TSIGenomeAssemblyMetadata,
    TSIHiCMetadata,
    TSIGenomicsDArTMetadata,
)

from .fungi.ingest import (
    FungiIlluminaShortreadMetadata,
    FungiONTPromethionMetadata,
    FungiMetabolomicsMetadata,
)

from .plant_pathogen.ingest import (
    PlantPathogenIlluminaShortreadMetadata,
    PlantPathogenPacbioHifiMetadata,
    PlantPathogenONTPromethionMetadata,
)

from .cipps.ingest import CIPPSIlluminaShortreadMetadata, CIPPSPacbioHifiMetadata

from .plant_protein_atlas.ingest import (
    PlantProteinAtlasPhenoCTXrayRawMetadata,
    PlantProteinAtlasPhenoCTXrayAnalysedMetadata,
    PlantProteinAtlasHyperspectralMetadata,
    PlantProteinAtlasASDSpectroMetadata,
    PlantProteinAtlasNutritionalMetadata,
    PlantProteinAtlasMetabolomicsMetadata,
    PlantProteinAtlasMetabolomicsAnalysedMetadata,
    PlantProteinAtlasProteomicsMetadata,
    PlantProteinAtlasProteomicsAnalysedMetadata,
    PlantProteinAtlasProteomicsDatabaseMetadata,
)

from .grasslands.ingest import (
    AGIlluminaShortreadMetadata,
    AGHiCMetadata,
    AGPacbioHifiMetadata,
    AGGenomicsDDRADMetadata,
)

from .collaborations.ingest import (
    CollaborationsMetagenomicsNovaseqMetadata,
    CollaborationsONTPromethionMetadata,
)

from .bpa_sample_data.ingest import (
    BSDSampleImagesMetadata,
)

from .workshop.ingest import (
    WorkshopPlantPathogenPacbioHifiMetadata,
    WorkshopFungiIlluminaShortreadMetadata,
)

from .avian.ingest import (
    AvianPacbioHifiMetadata,
    AvianHiCMetadata,
)

from .forest.ingest import (
    ForestPacbioHifiMetadata,
    ForestIlluminaShortreadMetadata,
)

from .ipm.ingest import (
    IPMIlluminaShortreadMetadata,
)


class ProjectInfo:
    projects = {
        "amd": [
            AustralianMicrobiomeMetagenomicsAnalysedMetadata,
            AustralianMicrobiomeMetagenomicsNovaseqMetadata,
            AustralianMicrobiomeMetagenomicsNovaseqControlMetadata,
            AustralianMicrobiomeAmpliconsMetadata,
            AustralianMicrobiomeAmpliconsControlMetadata,
        ],
        "ausarg": [
            AusargIlluminaFastqMetadata,
            AusargPacbioHifiMetadata,
            AusargONTPromethionMetadata,
            AusargExonCaptureMetadata,
            AusargHiCMetadata,
            AusargGenomicsDArTMetadata,
            AusargGenomicsDDRADMetadata,
        ],
        "base": [
            BASEAmpliconsMetadata,
            BASEAmpliconsControlMetadata,
            BASEMetagenomicsMetadata,
            BASESiteImagesMetadata,
        ],
        "gap": [
            GAPIlluminaShortreadMetadata,
            GAPONTMinionMetadata,
            GAPONTPromethionMetadata,
            GAPGenomics10XMetadata,
            GAPHiCMetadata,
            GAPGenomicsDDRADMetadata,
            GAPPacbioHifiMetadata,
        ],
        "gbr": [GbrAmpliconsMetadata, GbrPacbioMetadata],
        "marine-microbes": [
            MarineMicrobesAmpliconsMetadata,
            MarineMicrobesAmpliconsControlMetadata,
            MarineMicrobesMetagenomicsMetadata,
            MarineMicrobesMetatranscriptomeMetadata,
        ],
        "omg": [
            OMG10XRawMetadata,
            OMG10XRawIlluminaMetadata,
            OMG10XProcessedIlluminaMetadata,
            OMGExonCaptureMetadata,
            OMGWholeGenomeMetadata,
            OMGGenomicsNovaseqMetadata,
            OMGGenomicsHiSeqMetadata,
            OMGGenomicsDDRADMetadata,
            OMGGenomicsPacbioMetadata,
            OMGONTPromethionMetadata,
            OMGTranscriptomicsNextseq,
            OMGGenomicsPacBioGenomeAssemblyMetadata,
            OMGAnalysedDataMetadata,
            OMGGenomicsDArTMetadata,
        ],
        "tsi": [
            TSIPacbioHifiMetadata,
            TSIGenomicsDDRADMetadata,
            TSIIlluminaShortreadMetadata,
            TSIIlluminaFastqMetadata,
            TSIGenomeAssemblyMetadata,
            TSIHiCMetadata,
            TSIGenomicsDArTMetadata,
        ],
        "sepsis": [
            SepsisGenomicsMiseqMetadata,
            SepsisGenomicsPacbioMetadata,
            SepsisGenomicsAnalysedMetadata,
            SepsisTranscriptomicsAnalysedMetadata,
            SepsisTranscriptomicsHiseqMetadata,
            SepsisMetabolomicsLCMSMetadata,
            SepsisMetabolomicsGCMSMetadata,
            SepsisMetabolomicsAnalysedMetadata,
            SepsisProteomicsMS1QuantificationMetadata,
            SepsisProteomicsSwathMSMetadata,
            SepsisProteomicsSwathMSCombinedSampleMetadata,
            SepsisProteomicsSwathMSPoolMetadata,
            SepsisProteomics2DLibraryMetadata,
            SepsisProteomicsAnalysedMetadata,
            SepsisProteomicsProteinDatabaseMetadata,
        ],
        "stemcells": [
            StemcellsTranscriptomeMetadata,
            StemcellsSmallRNAMetadata,
            StemcellsSingleCellRNASeqMetadata,
            StemcellsMetabolomicsMetadata,
            StemcellsProteomicsMetadata,
            StemcellsProteomicsPoolMetadata,
            StemcellsProteomicsAnalysedMetadata,
            StemcellsMetabolomicsAnalysedMetadata,
            StemcellsTranscriptomeAnalysedMetadata,
        ],
        "wheat-cultivars": [
            WheatCultivarsMetadata,
        ],
        "wheat-pathogens": [
            WheatPathogensGenomesMetadata,  # the first half of wheat pathogens
        ],
        "fungi": [
            FungiIlluminaShortreadMetadata,
            FungiONTPromethionMetadata,
            FungiMetabolomicsMetadata,
        ],
        "pp": [
            PlantPathogenIlluminaShortreadMetadata,
            PlantPathogenPacbioHifiMetadata,
            PlantPathogenONTPromethionMetadata,
        ],
        "cipps": [
            CIPPSIlluminaShortreadMetadata,
            CIPPSPacbioHifiMetadata,
        ],
        "ppa": [
            PlantProteinAtlasPhenoCTXrayRawMetadata,
            PlantProteinAtlasPhenoCTXrayAnalysedMetadata,
            PlantProteinAtlasHyperspectralMetadata,
            PlantProteinAtlasASDSpectroMetadata,
            PlantProteinAtlasNutritionalMetadata,
            PlantProteinAtlasMetabolomicsMetadata,
            PlantProteinAtlasMetabolomicsAnalysedMetadata,
            PlantProteinAtlasProteomicsMetadata,
            PlantProteinAtlasProteomicsAnalysedMetadata,
            PlantProteinAtlasProteomicsDatabaseMetadata,
        ],
        "grasslands": [
            AGIlluminaShortreadMetadata,
            AGHiCMetadata,
            AGPacbioHifiMetadata,
            AGGenomicsDDRADMetadata,
        ],
        "collaborations": [
            CollaborationsMetagenomicsNovaseqMetadata,
            CollaborationsONTPromethionMetadata,
        ],
        "bsd": [
            BSDSampleImagesMetadata,
        ],
        "workshop": [
            WorkshopPlantPathogenPacbioHifiMetadata,
            WorkshopFungiIlluminaShortreadMetadata,
        ],
        "avian": [
            AvianPacbioHifiMetadata,
            AvianHiCMetadata,
        ],
        "forest": [
            ForestPacbioHifiMetadata,
            ForestIlluminaShortreadMetadata,
        ],
        "ipm": [
            IPMIlluminaShortreadMetadata,
        ],
    }

    def __init__(self):
        self.metadata_info = self._build_metadata_info()

    def _build_metadata_info(self):
        info = []
        slugs = set()
        for project_name, classes in ProjectInfo.projects.items():
            for cls in classes:
                class_info = {
                    t: getattr(cls, t, None)
                    for t in ("omics", "technology", "organization")
                }
                class_info.update(
                    {t: getattr(cls, t, False) for t in ("analysed", "pool")}
                )
                class_info["project"] = project_name
                class_info["cls"] = cls

                class_info["slug"] = slug = self._make_slug(class_info)
                # ensure that 'slug' is unique
                assert slug not in slugs
                slugs.add(slug)
                info.append(class_info)
        return info

    def _make_slug(self, class_info):
        nm_parts = [class_info[t] for t in ("project", "omics", "technology")]
        if class_info["analysed"]:
            nm_parts.append("analysed")
        if class_info["pool"]:
            nm_parts.append("pool")
        return "-".join(filter(None, nm_parts))

    def cli_options(self):
        return dict((t["slug"], t["cls"]) for t in self.metadata_info)
