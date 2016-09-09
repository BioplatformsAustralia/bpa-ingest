from .base.ingest import SoilMetadata
from .gbr_amplicon.ingest import GbrAmpliconMetadata
from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_pathogens.ingest import WheatPathogensMetadata

PROJECTS = {
    'base': SoilMetadata,
    'wheat-cultivars': WheatCultivarsMetadata,
    'wheat-pathogens': WheatPathogensMetadata,
    'gbr-amplicon': GbrAmpliconMetadata,
}
