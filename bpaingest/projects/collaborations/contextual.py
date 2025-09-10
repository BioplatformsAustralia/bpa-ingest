import re
from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    make_field_definition as fld,
    make_skip_column as skp,
    FieldDefinition,
    ExcelWrapper,
)
from ...abstract import BaseDatasetControlContextual
from ...abstract import BaseLibraryContextual


class CollaborationsDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/collaborations/dataset_control/2024-05-21/"
    ]
    name = "collaborations-dataset-contextual"
    sheet_names = [
        "Sheet1",
    ]
    contextual_linkage = ("bioplatforms_library_id",)
    related_data_identifier_type = "sample_id"
    additional_fields = [
        fld("bioplatforms_sample_id", "bioplatforms_sample_id"),
        fld("bioplatforms_dataset_id", "bioplatforms_dataset_id"),
        fld("bioplatforms_project_code", "bioplatforms_project_code"),
        fld("bioplatforms_project", "bioplatforms_project"),
        fld("related_data_doi", "related_data_doi", coerce=ingest_utils.get_clean_doi),
        fld(
            "related_data_identifier",
            "related_data_identifier",
            coerce=ingest_utils.extract_ands_id,
        ),
    ]

    def _read_metadata(self, metadata_path):
        metadata = super()._read_metadata(metadata_path)
        for linkage in metadata:
            doi = metadata[linkage].get("related_data_doi", "")
            identifier = metadata[linkage].get("related_data_identifier", "")
            if identifier:
                identifier = "{}:{}".format(
                    self.related_data_identifier_type, identifier
                )
            related = metadata[linkage].get("related_data", "")
            metadata[linkage]["related_data"] = " ".join(
                filter(None, (related, doi, identifier))
            )
            del metadata[linkage]["related_data_doi"]
            del metadata[linkage]["related_data_identifier"]
        return metadata

    def library_ids(self):
        if len(self.contextual_linkage) != 1:
            raise Exception("Linkage of unexpected length")

        # return a list of the first item of the linkage
        # This will be a BPA Sample ID
        return list(k[0] for k in self.dataset_metadata.keys())


class CollaborationsLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/collaborations/metadata/2024-05-14/"
    ]
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]
    name = "collaborations-library-contextual"
    sheet_name = "Sheet1"
    metadata_unique_identifier = "sample_id"
    source_pattern = "/*.xlsx"

    field_spec = [
        fld("bioplatforms_project", "bioplatforms_project"),
        fld("bioplatforms_project_code", "bioplatforms_project_code"),
        fld("sample_id", "sample_id", coerce=ingest_utils.extract_ands_id),
        fld(
            "utc_date_sampled",
            "utc_date_sampled",
            coerce=ingest_utils.get_date_isoformat,
        ),
        fld("utc_time_sampled", "utc_time_sampled", coerce=ingest_utils.get_time),
        fld(
            "collection_date",
            "collection_date",
            coerce=ingest_utils.get_date_isoformat,
        ),
        fld(
            "longitude",
            "longitude",
            units="decimal_degrees",
            coerce=ingest_utils.get_clean_number,
        ),
        fld(
            "latitude",
            "latitude",
            units="decimal_degrees",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("lat_lon", "lat_lon", units="decimal_degrees"),
        fld("geo_loc_name", "geo_loc_name"),
        fld("sample_site_location_description", "sample_site_location_description"),
        fld(
            "sample_submission_date",
            "sample_submission_date",
            coerce=ingest_utils.get_date_isoformat_as_datetime,  # Note this can be removed when ckan handles Z tz
        ),
        fld("sample_submitter", "sample_submitter"),
        fld("sample_attribution", "sample_attribution"),
        fld("funding_agency", "funding_agency"),
        fld("samp_collect_device", "samp_collect_device"),
        fld("samp_mat_process", "samp_mat_process"),
        fld("store_cond", "store_cond"),
        fld("biotic_relationship", "biotic_relationship"),
        fld("env_medium", "env_medium"),
        fld("env_broad_scale", "env_broad_scale"),
        fld("env_local_scale", "env_local_scale"),
        fld("general_env_feature", "general_env_feature"),
        fld("vegetation_type", "vegetation_type"),
        fld("notes", "notes"),
        fld(
            "depth_lower",
            "depth_lower",
            units="m",
            coerce=ingest_utils.get_clean_number,
        ),
        fld(
            "depth_upper",
            "depth_upper",
            units="m",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("depth", "depth", units="m", coerce=ingest_utils.get_clean_number),
        fld("sample_type", "sample_type"),
        fld(
            "ammonium_nitrogen_wt",
            "ammonium_nitrogen_wt",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("ammonium_nitrogen_wt_meth", "ammonium_nitrogen_wt_meth"),
        fld(
            "boron_hot_cacl2",
            "boron_hot_cacl2",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("boron_hot_cacl2_meth", "boron_hot_cacl2_meth"),
        fld("clay", "clay", units="%", coerce=ingest_utils.get_percentage),
        fld("clay_meth", "clay_meth"),
        fld("color", "color"),
        fld("color_meth", "color_meth"),
        fld(
            "conductivity",
            "conductivity",
            units="dS/m",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("conductivity_meth", "conductivity_meth"),
        fld(
            "coarse_sand",
            "coarse_sand",
            units="%",
            coerce=ingest_utils.get_percentage,
        ),
        fld("coarse_sand_meth", "coarse_sand_meth"),
        fld("collection_permit", "collection_permit", optional=True),
        fld("crop_rotation_1yr_since_present", "crop_rotation_1yr_since_present"),
        fld("crop_rotation_2yrs_since_present", "crop_rotation_2yrs_since_present"),
        fld("crop_rotation_3yrs_since_present", "crop_rotation_3yrs_since_present"),
        fld("crop_rotation_4yrs_since_present", "crop_rotation_4yrs_since_present"),
        fld("crop_rotation_5yrs_since_present", "crop_rotation_5yrs_since_present"),
        fld(
            "density",
            "density",
            units="kg/m3",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("density_meth", "density_meth"),
        fld(
            "dna_concentration",
            "dna_concentration",
            units="ng/" + "\u03BC" + "L",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("dna_concentration_method", "dna_concentration_method"),
        fld(
            "dtpa_copper",
            "dtpa_copper",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("dtpa_copper_meth", "dtpa_copper_meth"),
        fld(
            "dtpa_iron",
            "dtpa_iron",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("dtpa_iron_meth", "dtpa_iron_meth"),
        fld(
            "dtpa_manganese",
            "dtpa_manganese",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("dtpa_manganese_meth", "dtpa_manganese_meth"),
        fld(
            "dtpa_zinc",
            "dtpa_zinc",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("dtpa_zinc_meth", "dtpa_zinc_meth"),
        fld("elev", "elev", units="m", coerce=ingest_utils.get_clean_number),
        fld(
            "exc_aluminium",
            "exc_aluminium",
            units="meq/100g",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("exc_aluminium_meth", "exc_aluminium_meth"),
        fld(
            "exc_calcium",
            "exc_calcium",
            units="meq/100g",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("exc_calcium_meth", "exc_calcium_meth"),
        fld(
            "exc_magnesium",
            "exc_magnesium",
            units="meq/100g",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("exc_magnesium_meth", "exc_magnesium_meth"),
        fld(
            "exc_potassium",
            "exc_potassium",
            units="meq/100g",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("exc_potassium_meth", "exc_potassium_meth"),
        fld(
            "exc_sodium",
            "exc_sodium",
            units="meq/100g",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("exc_sodium_meth", "exc_sodium_meth"),
        fld("fine_sand", "fine_sand", units="%", coerce=ingest_utils.get_percentage),
        fld("fine_sand_meth", "fine_sand_meth"),
        fld(
            "gravel",
            "gravel",
            units="%",
        ),
        fld("gravel_meth", "gravel_meth"),
        fld("hyperspectral_analysis", "hyperspectral_analysis", optional=True),
        fld(
            "hyperspectral_analysis_meth", "hyperspectral_analysis_meth", optional=True
        ),
        fld("local_class", "local_class"),
        fld("local_class_meth", "local_class_meth"),
        fld(
            "microbial_biomass",
            "microbial_biomass",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("microbial_biomass_meth", "microbial_biomass_meth"),
        fld(
            "nitrate_nitrogen",
            "nitrate_nitrogen",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("nitrate_nitrogen_meth", "nitrate_nitrogen_meth"),
        fld(
            "organic_carbon",
            "organic_carbon",
            units="%",
            coerce=ingest_utils.get_percentage,
        ),
        fld("organic_carbon_meth", "organic_carbon_meth"),
        fld(
            "ph",
            "ph",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("ph_meth", "ph_meth"),
        fld(
            "ph_solid_h2o",
            "ph_solid_h2o",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("ph_solid_h2o_meth", "ph_solid_h2o_meth"),
        fld(
            "phosphorus_colwell",
            "phosphorus_colwell",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("phosphorus_colwell_meth", "phosphorus_colwell_meth"),
        fld(
            "potassium_colwell",
            "potassium_colwell",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("potassium_colwell_meth", "potassium_colwell_meth"),
        fld("sand", "sand", units="%", coerce=ingest_utils.get_percentage),
        fld("sand_meth", "sand_meth"),
        fld("silt", "silt", units="%", coerce=ingest_utils.get_percentage),
        fld("silt_meth", "silt_meth"),
        fld(
            "slope_gradient",
            "slope_gradient",
            units="%",
            coerce=ingest_utils.get_percentage,
        ),
        fld(
            "sulphur",
            "sulphur",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("sulphur_meth", "sulphur_meth"),
        fld("synonyms", "synonyms"),
        fld("texture", "texture"),
        fld("texture_meth", "texture_meth"),
        fld(
            "total_nitrogen",
            "total_nitrogen",
            units="mg/kg",
            coerce=ingest_utils.get_clean_number,
        ),
        fld("total_nitrogen_meth", "total_nitrogen_meth"),
        fld(
            "vegetation_dom_grasses",
            "vegetation_dom_grasses",
            units="%",
        ),
        fld("vegetation_dom_grasses_meth", "vegetation_dom_grasses_meth"),
        fld(
            "vegetation_dom_shrubs",
            "vegetation_dom_shrubs",
            units="%",
        ),
        fld("vegetation_dom_shrubs_meth", "vegetation_dom_shrubs_meth"),
        fld(
            "vegetation_dom_trees",
            "vegetation_dom_trees",
            units="%",
        ),
        fld("vegetation_dom_trees_meth", "vegetation_dom_trees_meth"),
        fld(
            "vegetation_total_cover",
            "vegetation_total_cover",
            units="%",
            coerce=ingest_utils.get_percentage,
        ),
        fld(
            "water_content",
            "water_content",
            units="%",
            coerce=ingest_utils.get_percentage,
        ),
        fld("water_content_soil_meth", "water_content_soil_meth"),
        fld("sample_metadata_ingest_file", "sample_metadata_ingest_file"),
        fld("sample_metadata_update_history", "sample_metadata_update_history"),
        fld("sample_database_file", "sample_database_file"),
    ]

class CollaborationsCaneToadDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/collaborations/dataset_control/2025-09-08/"
    ]
    name = "collaborations-canetoad-dataset-contextual"
    sheet_names = [
        "Data control",
    ]
    contextual_linkage = ("bioplatforms_sample_id",)
    related_data_identifier_type = "bioplatforms_sample_id"
    additional_fields = [
        fld("bioplatforms_libray_id", "bioplatforms_library_id"),
        fld("bioplatforms_dataset_id", "bioplatforms_dataset_id"),
        fld("bioplatforms_project_code", "bioplatforms_project_code"),
        fld('data_type', 'data_type')
    ]


    def library_ids(self):
        if len(self.contextual_linkage) != 1:
            raise Exception("Linkage of unexpected length")

        # return a list of the first item of the linkage
        # This will be a BPA Sample ID
        return list(k[0] for k in self.dataset_metadata.keys())

class CollaborationsCaneToadLibraryContextual(BaseLibraryContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/collaborations/metadata/2025-09-08/"
    ]
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]
    name = "collaborations-canetoad-library-contextual"
    sheet_name = "Sample Metadata"
    metadata_unique_identifier = "bioplatforms_sample_id"
    source_pattern = "/*.xlsx"

    field_spec = [
            fld('bioplatforms_sample_id', 'bioplatforms_sample_id', coerce=ingest_utils.extract_ands_id),
            fld('specimen_id', 'specimen_id'),
            fld('specimen_id_description', 'specimen_id_description'),
            fld('tissue_number', 'tissue_number'),
            fld('institution_name', 'institution_name'),
            fld('tissue_collection', 'tissue_collection'),
            fld('tissue_collection_type', 'tissue_collection_type'),
            fld('sample_type', 'sample_type'),
            fld('sample_custodian', 'sample_custodian'),
            fld('access_rights', 'access_rights'),
            fld('tissue_type', 'tissue_type'),
            fld('tissue_preservation', 'tissue_preservation'),
            fld('tissue_preservation_temperature', 'tissue_preservation_temperature'),
            fld('sample_quality', 'sample_quality'),
            fld('taxon_id', 'taxon_id'),
            fld('phylum', 'phylum'),
            fld('klass', 'class'),
            fld('order', 'order'),
            fld('family', 'family'),
            fld('genus', 'genus'),
            fld('species', 'species'),
            fld('subspecies', 'subspecies'),
            fld('scientific_name', 'scientific_name'),
            fld('scientific_name_note', 'scientific_name_note'),
            fld('scientific_name_authorship', 'scientific_name_authorship'),
            fld('common_name', 'common_name'),
            fld('collection_date', 'collection_date', coerce=ingest_utils.get_date_isoformat),
            fld('collector', 'collector'),
            fld('collection_method', 'collection_method'),
            fld('collector_sample_id', 'collector_sample_id'),
            fld('wild_captive', 'wild_captive'),
            fld('source_population', 'source_population'),
            fld('country', 'country'),
            fld('state_or_region', 'state_or_region'),
            fld('location_text', 'location_text'),
            fld('habitat', 'habitat'),
            fld('decimal_latitude_public', 'decimal_latitude_public'),
            fld('decimal_longitude_public', 'decimal_longitude_public'),
            fld('genotypic_sex', 'genotypic sex'),
            fld('phenotypic_sex', 'phenotypic sex'),
            fld('method_of_determination', 'method of determination'),
            fld('certainty', 'certainty'),
            fld('life_stage', 'life_stage'),
            fld('birth_date', 'birth_date', coerce=ingest_utils.get_date_isoformat),
            fld('death_date', 'death_date', coerce=ingest_utils.get_date_isoformat),
            fld('health_state', 'health_state'),
            fld('associated_media', 'associated_media'),
            fld('ancillary_notes', 'ancillary_notes'),
            fld('barcode_id', 'barcode_id'),
            fld('ala_specimen_url', 'ala_specimen_url'),
            fld('prior_genetics', 'prior_genetics'),
            fld('material_extraction_type', 'material_extraction_type'),
            fld('material_extraction_date', 'material_extraction_date', coerce=ingest_utils.get_date_isoformat),
            fld('material_extracted_by', 'material_extracted_by'),
            fld('material_extraction_method', 'material_extraction_method'),
            fld('material_conc_ng_ul', 'material_conc_ng_ul'),
            fld('taxonomic_group', 'taxonomic_group'),
            fld('type_status', 'type_status'),
]
