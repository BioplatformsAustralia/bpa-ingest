from .gbr_amplicon.ingest import GbrAmpliconMetadata
from .gbr_amplicon.download import download as download_gbr_amplicon

from .wheat_cultivars.ingest import WheatCultivarsMetadata
from .wheat_cultivars.download import download as download_wheatcultivars

from .wheat_pathogens.ingest import WheatPathogensMetadata
from .wheat_pathogens.download import download as download_wheat_pathogens

sync_handlers = {
    'wheat-cultivars': (download_wheatcultivars, WheatCultivarsMetadata, None),
    'wheat-pathogens': (download_wheat_pathogens, WheatPathogensMetadata, None),
    'gbr-amplicon': (download_gbr_amplicon, GbrAmpliconMetadata, lambda: ('bpa', get_password('gbr'))),
}


