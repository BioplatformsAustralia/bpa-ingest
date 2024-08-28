from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual


class PlantProteinAtlasDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/dataset_control/2024-08-27/"
    ]
    name = "ppa-dataset-contextual"
    contextual_linkage = ("dataset_id",)
    additional_fields = [
        fld(
            "library_id",
            "library_id",
            coerce=ingest_utils.extract_ands_id,
        ),
        fld(
            "sample_id",
            "sample_id",
            coerce=ingest_utils.extract_ands_id,
        ),
    ]


class PlantProteinAtlasLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ppa_staging/metadata/2023-08-09/"
    ]
    name = "ppa-library-contextual"
    metadata_unique_identifier = "bioplatforms_sample_id"
    contextual_linkage = ("bioplatforms_sample_id",)
    field_spec = [
        fld("planting_season", "planting_season", coerce=ingest_utils.get_int),
        fld("planting_site", "planting_site"),
        fld("planting_code", "planting_code"),
        fld("planting_block", "planting_block", coerce=ingest_utils.get_int),
        fld("planting_row", "planting_row", coerce=ingest_utils.get_int),
        fld("planting_bay", "planting_bay", coerce=ingest_utils.get_int),
        fld("variety_commercial", "variety_commercial"),
        fld("variety_name", "variety_name"),
        fld("plant_replicate", "plant_replicate", coerce=ingest_utils.get_int),
        fld(
            "bioplatforms_sample_id",
            "bioplatforms_sample_id",
            coerce=ingest_utils.extract_ands_id,
        ),
        fld("specimen_custodian", "specimen_custodian"),
        fld("sample_collection_type", "sample_collection_type"),
        fld("sample_type", "sample_type"),
        fld("taxon_id", "taxon_id", coerce=ingest_utils.get_int),
        fld("phylum", "phylum"),
        fld("klass", "class"),
        fld("order", "order"),
        fld("family", "family"),
        fld("genus", "genus"),
        fld("species", "species"),
        fld("sub_species", "sub_species"),
        fld("scientific_name", "scientific_name"),
        fld("scientific_name_authorship", "scientific_name_authorship"),
        fld("scientific_name_note", "scientific_name_note"),
        fld("common_name", "common_name"),
        fld("taxonomic_group", "taxonomic_group"),
        fld("tissue", "tissue"),
        fld("tissue_preservation", "tissue_preservation"),
        fld("tissue_preservation_temperature", "tissue_preservation_temperature"),
        fld("sample_quality", "sample_quality"),
        fld("wild_captive", "wild_captive"),
        fld(
            "collection_date", "collection_date", coerce=ingest_utils.get_date_isoformat
        ),
        fld("collector", "collector"),
        fld("collection_method", "collection_method"),
        fld("collection_permit", "collection_permit"),
        fld("collector_sample_id", "collector_sample_id"),
        fld("country", "country"),
        fld("state_or_region", "state_or_region"),
        fld("location_text", "location_text"),
        fld(
            "decimal_latitude_private",
            "decimal_latitude_private",
            coerce=ingest_utils.get_clean_number,
        ),
        fld(
            "decimal_longitude_private",
            "decimal_longitude_private",
            coerce=ingest_utils.get_clean_number,
        ),
        fld(
            "coord_uncertainty_metres",
            "coord_uncertainty_metres",
            coerce=ingest_utils.get_clean_number,
        ),
        fld(
            "decimal_latitude_public",
            "decimal_latitude_public",
            coerce=ingest_utils.get_clean_number,
        ),
        fld(
            "decimal_longitude_public",
            "decimal_longitude_public",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("health_state", "health_state"),
        fld("ancillary_notes", "ancillary_notes"),
    ]
