from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual


class DinoflagellatesDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/dinoflagellates/dataset_control/2026-08-04/"
    ]
    name = "dinoflagellates-dataset-contextual"
    contextual_linkage = ("sample_id",)
    additional_fields = [
        fld("library_id", "library_id"),
        fld("dataset_id", "dataset_id"),
    ]


class DinoflagellatesLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/dinoflagellates/metadata/2026-08-04/"
    ]
    name = "dinoflagellates-library-contextual"


