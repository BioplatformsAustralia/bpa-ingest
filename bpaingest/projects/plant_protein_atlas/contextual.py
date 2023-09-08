from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual


class PlantProteinAtlasDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/dataset_control/2023-08-09/"
    ]
    name = "ppa-dataset-contextual"
    contextual_linkage = ("sample_id",)
    additional_fields = [
        fld('library_id', 'library_id'),
        fld('dataset_id', 'dataset_id'),
    ]


class PlantProteinAtlasLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/metadata/2023-08-09/"
    ]
    name = "ppa-library-contextual"

