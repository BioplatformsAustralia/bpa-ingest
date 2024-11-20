import re
from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual


class BSDDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/bpa_sample_data/dataset_control/2024-08-05/"
    ]
    name = "bsd-dataset-contextual"
    contextual_linkage = ("sample_id",)
    """ library and dataset fields are skipped as they are blank for TSI.
    Because the linkage is sample id, if there are multiple dataset/libraries for the one sample, the dataset and
    library ids must be blank (to avoid duplicates), but we don't want to overwrite the ids in the ckan object.
    """
    additional_fields = [skp("library_id"), skp("dataset_id")]


class BSDLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/bpa_sample_data/metadata/2024-11-20/"
    ]
    name = "bsd-library-contextual"
    metadata_unique_identifier = "bioplatforms_sample_id"
    field_spec = [
        fld(
            "bioplatforms_sample_id",
            re.compile(r"(bioplatforms_)?sample_[Ii][Dd]"),
            coerce=ingest_utils.extract_ands_id,
        ),
        fld("specimen_custodian", "specimen_custodian"),
        fld("specimen_id", "specimen_id"),
        fld("specimen_id_description", "specimen_id_description"),
        fld("sample_custodian", "sample_custodian"),
        fld("sample_id", "sample_id", coerce=ingest_utils.extract_ands_id),
        fld("sample_id_description", "sample_id_description"),
        fld("sample_collection_type", "sample_collection_type"),
        fld("sample_type", "sample_type"),
        fld("taxon_id", "taxon_id"),
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
        fld("isolate", "isolate"),
        fld("host_common_name", "host_common_name"),
        fld("host_family", "host_family"),
        fld("host_scientific_name", "host_scientific_name"),
        fld("host_organ", "host_organ"),
        fld("host_symptom", "host_symptom"),
        fld("host_status", "host_status"),
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
        fld("identified_by", "identified_by"),
        fld("source_population", "source_population"),
        fld("country", "country"),
        fld("state_or_region", "state_or_region"),
        fld("indigenous_location", "indigenous_location"),
        fld("location_text", "location_text"),
        fld("env_broad_scale", "env_broad_scale"),
        fld("env_local_scale", "env_local_scale"),
        fld("env_medium", "env_medium"),
        fld("altitude", "altitude"),
        fld("depth", "depth", coerce=ingest_utils.get_clean_number),
        fld("temperature", "temperature"),
        fld("habitat", "habitat"),
        fld("location_info_restricted", "location_info_restricted"),
        fld("decimal_latitude_private", "decimal_latitude_private"),
        fld("decimal_longitude_private", "decimal_longitude_private"),
        fld("coord_uncertainty_metres", "coord_uncertainty_metres"),
        fld("decimal_latitude_public", "decimal_latitude_public"),
        fld("decimal_longitude_public", "decimal_longitude_public"),
        fld("life_stage", "life_stage"),
        fld("health_state", "health_state"),
        fld("associated_media", "associated_media"),
        fld("ancillary_notes", "ancillary_notes"),
        fld("type_status", "type_status"),
        fld("material_extraction_type", "material_extraction_type"),
        fld(
            "material_extraction_date",
            "material_extraction_date",
            coerce=ingest_utils.get_date_isoformat,
        ),
        fld("material_extracted_by", "material_extracted_by"),
        fld("material_extraction_method", "material_extraction_method"),
        fld("material_conc_ng_ul", "material_conc_ng_ul"),
    ]
