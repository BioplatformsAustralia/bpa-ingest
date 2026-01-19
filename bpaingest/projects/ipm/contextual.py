from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual


class IPMDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ipm_staging/dataset_control/2025-04-10/"
    ]
    name = "ipm-dataset-contextual"
    contextual_linkage = ("sample_id",)
    additional_fields = [
        fld("library_id", "library_id"),
        fld("dataset_id", "dataset_id"),
    ]


class IPMLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ipm_staging/metadata/2026-01-20/"
    ]
    name = "ipm-library-contextual"


