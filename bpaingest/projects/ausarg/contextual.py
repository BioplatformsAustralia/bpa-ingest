import re
from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual


class AusargDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/dataset_control/2021-11-24/"
    ]
    name = "ausarg-dataset-contextual"
    contextual_linkage = ("sample_id",)
    additional_fields = [
        fld("library_id", "library_id"),
        fld("dataset_id", "dataset_id"),
    ]


class AusargLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/ausarg_staging/metadata/2024-05-23/"
    ]
    name = "ausarg-library-contextual"
    metadata_unique_identifier = "sample_id"
    sheet_names = ["sample_metadata"]

    field_spec = [
        fld(
            "sample_id",
            re.compile(r"sample_[Ii][Dd]"),
            coerce=ingest_utils.extract_ands_id,
        ),
        fld("specimen_id", re.compile(r"specimen_?[Ii][Dd]")),
        fld("specimen_id_description", re.compile(r"specimen_?[Ii][Dd]_description")),
        fld("tissue_number", "tissue_number"),
        fld("voucher_or_tissue_number", "voucher_or_tissue_number"),
        fld("institution_name", "institution_name"),
        fld("tissue_collection", "tissue_ collection"),
        fld("sample_custodian", "sample_custodian"),
        fld("access_rights", "access_rights"),
        fld("tissue_type", "tissue_type"),
        fld("tissue_preservation", "tissue_preservation"),
        fld("sample_quality", "sample_quality"),
        fld("taxon_id", re.compile(r"taxon_[Ii][Dd]"), coerce=ingest_utils.get_int),
        fld("phylum", "phylum"),
        fld("klass", "class"),
        fld("order", "order"),
        fld("family", "family"),
        fld("genus", "genus"),
        fld("species", "species"),
        fld("subspecies", "subspecies"),
        fld("common_name", "common_name"),
        fld("identified_by", "identified_by"),
        fld(
            "collection_date",
            "collection_date",
            coerce=ingest_utils.get_date_isoformat,
        ),
        fld("collector", "collector"),
        fld("collection_method", "collection_method"),
        fld("collector_sample_id", re.compile(r"collector_sample_[Ii][Dd]")),
        fld("wild_captive", "wild_captive"),
        fld("source_population", "source_population"),
        fld("country", "country"),
        fld("state_or_region", "state_or_region"),
        fld("location_text", "location_text"),
        fld("habitat", "habitat"),
        # skip the private lat/long as this will contain data not to be shared
        skp("decimal_latitude_private"),
        skp("decimal_longitude_private"),
        # decimal_latitude
        fld(
            "decimal_latitude",
            "decimal_latitude_public",
            coerce=ingest_utils.get_clean_number,
        ),
        fld(
            "decimal_latitude_public",
            "decimal_latitude_public",
            coerce=ingest_utils.get_clean_number,
        ),
        # decimal_longitude
        fld(
            "decimal_longitude",
            "decimal_longitude_public",
            coerce=ingest_utils.get_clean_number,
        ),
        fld(
            "decimal_longitude_public",
            "decimal_longitude_public",
            coerce=ingest_utils.get_clean_number,
        ),
        fld(
            "coord_uncertainty_metres",
            "coord_uncertainty_metres",
            optional=True,
        ),
        fld("genotypic_sex", "genotypic sex"),
        fld("phenotypic_sex", "phenotypic sex"),
        fld("method_of_determination", "method of determination"),
        fld("certainty", "certainty"),
        fld("lifestage", "life-stage"),
        fld("birth_date", "birth_date", coerce=ingest_utils.get_date_isoformat),
        fld("death_date", "death_date", coerce=ingest_utils.get_date_isoformat),
        fld("associated_media", "associated_media"),
        fld("ancillary_notes", "ancillary_notes"),
        fld("barcode_id", "barcode_id"),
        fld("ala_specimen_url", re.compile(r"[aA][Ll][aA]_specimen_[uU][rR][lL]")),
        fld("prior_genetics", "prior_genetics"),
        fld("taxonomic_group", "taxonomic_group"),
        fld("type_status", "type_status"),
        fld("material_extraction_type", re.compile(r"[Mm]aterial_extraction_type")),
        fld(
            "material_extraction_date",
            re.compile(r"[Mm]aterial_extraction_date"),
            coerce=ingest_utils.get_date_isoformat,
        ),
        fld("material_extracted_by", re.compile(r"[Mm]aterial_extracted_by")),
        fld(
            "material_extraction_method",
            re.compile(r"[Mm]aterial_extraction_method"),
        ),
        fld("material_conc_ng_ul", re.compile(r"[Mm]aterial_conc_ng_ul")),
        skp("notes_pm"),
    ]
