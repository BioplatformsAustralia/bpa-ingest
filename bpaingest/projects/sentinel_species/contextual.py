from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual


class SentinelSpeciesDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sentinel_species/dataset_control/2026-08-11/"
    ]
    name = "sentinel-species-dataset-contextual"
    contextual_linkage = ("sample_id",)
    additional_fields = [
        fld("library_id", "library_id"),
        fld("dataset_id", "dataset_id"),
    ]


class SentinelSpeciesLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sentinel_species/metadata/2026-07-17/"
    ]
    name = "sentinel-species-library-contextual"


