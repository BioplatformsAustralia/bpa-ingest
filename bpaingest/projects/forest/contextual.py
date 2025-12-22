import re
from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual


class ForestDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/forest_staging/dataset_control/2025-04-02/"
    ]
    name = "forest-dataset-contextual"
    contextual_linkage = ("library_id",)
    additional_fields = [
        fld("sample_id", "sample_id"),
        fld("dataset_id", "dataset_id"),
    ]


class ForestLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/forest_staging/metadata/2025-11-28/"
    ]
    name = "forest-library-contextual"
    metadata_unique_identifier = "bioplatforms_library_id"
    sheet_names = [
        "PacBio",
        "Re-sequencing",
    ]
    field_spec = [
        fld('bioplatforms_sample_id', 'bioplatforms_sample_id', coerce=ingest_utils.extract_ands_id,),
        fld('bioplatforms_library_id', 'bioplatforms_library_id', coerce=ingest_utils.extract_ands_id,),
        fld('bioplatforms_dataset_id', 'bioplatforms_dataset_id', coerce=ingest_utils.extract_ands_id,),
        fld('specimen_custodian', 'specimen_custodian'),
        fld('specimen_id', 'specimen_id'),
        fld('specimen_id_description', 'specimen_id_description'),
        fld('sample_custodian', 'sample_custodian'),
        fld('sample_id', 'sample_id'),
        fld('sample_id_description', 'sample_id_description'),
        fld('sample_collection_type', 'sample_collection_type'),
        fld('sample_type', 'sample_type'),
        fld('taxon_id', 'taxon_id', coerce=ingest_utils.get_int),
        fld('phylum', 'phylum'),
        fld('klass', 'class'),
        fld('order', 'order'),
        fld('family', 'family'),
        fld('genus', 'genus'),
        fld('species', 'species'),
        fld('sub_species', 'sub_species'),
        fld('scientific_name', 'scientific_name'),
        fld('scientific_name_authorship', 'scientific_name_authorship'),
        fld('scientific_name_note', 'scientific_name_note'),
        fld('common_name', 'common_name'),
        fld('taxonomic_group', 'taxonomic_group'),
        fld('tissue', 'tissue'),
        fld('tissue_preservation', 'tissue_preservation'),
        fld('tissue_preservation_temperature', 'tissue_preservation_temperature'),
        fld('sample_quality', 'sample_quality'),
        fld('wild_captive', 'wild_captive'),
        fld('collection_date', 'collection_date', coerce=ingest_utils.get_date_isoformat),
        fld('collector', 'collector'),
        fld('collection_method', 'collection_method'),
        fld('collection_permit', 'collection_permit'),
        fld('collector_sample_id', 'collector_sample_id'),
        fld('identified_by', 'identified_by'),
        fld('source_population', 'source_population'),
        fld('country', 'country'),
        fld('state_or_region', 'state_or_region'),
        fld('indigenous_location', 'indigenous_location'),
        fld('location_text', 'location_text'),
        fld('env_broad_scale', 'env_broad_scale'),
        fld('env_local_scale', 'env_local_scale'),
        fld('env_medium', 'env_medium'),
        fld('altitude', 'altitude'),
        fld('depth', 'depth', coerce=ingest_utils.get_clean_number),
        fld('temperature', 'temperature'),
        fld('habitat', 'habitat'),
        fld('location_info_restricted', 'location_info_restricted'),
        fld('coord_uncertainty_metres', 'coord_uncertainty_metres'),
        fld('decimal_latitude_public', 'decimal_latitude_public'),
        fld('decimal_longitude_public', 'decimal_longitude_public'),
        fld('life_stage', 'life_stage'),
        fld('health_state', 'health_state'),
        fld('associated_media', 'associated_media'),
        fld('ancillary_notes', 'ancillary_notes'),
        fld('type_status', 'type_status'),
        fld('material_extraction_type', 'material_extraction_type'),
        fld('material_extraction_date', 'material_extraction_date', coerce=ingest_utils.get_date_isoformat),
        fld('material_extracted_by', 'material_extracted_by'),
        fld('material_extraction_method', 'material_extraction_method'),
        fld('material_conc_ng_ul', 'material_conc_ng_ul'),
        fld('project_aim', 'project_aim'),
        fld('sample_submitter_name', 'sample_submitter_name'),
        fld('sample_submitter_email', 'sample_submitter_email'),
        fld('sample_submission_date', 'sample_submission_date', coerce=ingest_utils.get_date_isoformat),

    ]

