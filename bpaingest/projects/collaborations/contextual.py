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


"""
            fld('bioplatforms_project', 'bioplatforms_project'),
            fld('bioplatforms_project_code', 'bioplatforms_project_code'),
            fld('sample_id', 'sample_id', coerce=ingest_utils.extract_ands_id),
            fld('collection_date', 'collection_date', coerce=ingest_utils.get_date_isoformat),
            fld('utc_date_sampled', 'utc_date_sampled', coerce=ingest_utils.get_date_isoformat),
            fld('utc_time_sampled', 'utc_time_sampled', coerce=ingest_utils.get_time),
            fld('collection_permit', 'collection_permit'),
            fld('funding_agency', 'funding_agency'),
            fld('sample_submitter', 'sample_submitter'),
            fld('sample_attribution', 'sample_attribution'),
            fld('sample_type', 'sample_type'),
            fld('synonyms', 'synonyms'),
            fld('geo_loc_name', 'geo_loc_name'),
            fld('lat_lon', 'lat_lon'),
            fld('latitude', 'latitude', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude', coerce=ingest_utils.get_clean_number),
            fld('sample_site_location_description', 'sample_site_location_description'),
            fld('sample_submission_date', 'sample_submission_date', coerce=ingest_utils.get_date_isoformat),
            fld('store_cond', 'store_cond'),
            fld('depth', 'depth', coerce=ingest_utils.get_clean_number),
            fld('depth_lower', 'depth_lower'),
            fld('depth_upper', 'depth_upper'),
            fld('elev', 'elev'),
            fld('env_broad_scale', 'env_broad_scale'),
            fld('env_local_scale', 'env_local_scale'),
            fld('env_medium', 'env_medium'),
            fld('notes', 'notes'),
            fld('local_class', 'local_class'),
            fld('local_class_meth', 'local_class_meth'),
            fld('general_env_feature', 'general_env_feature'),
            fld('vegetation_type', 'vegetation_type'),
            fld('vegetation_dom_grasses', 'vegetation_dom_grasses'),
            fld('vegetation_dom_grasses_meth', 'vegetation_dom_grasses_meth'),
            fld('vegetation_dom_shrubs', 'vegetation_dom_shrubs'),
            fld('vegetation_dom_shrubs_meth', 'vegetation_dom_shrubs_meth'),
            fld('vegetation_dom_trees', 'vegetation_dom_trees'),
            fld('vegetation_dom_trees_meth', 'vegetation_dom_trees_meth'),
            fld('vegetation_total_cover', 'vegetation_total_cover'),
            fld('sample_database_file', 'sample_database_file'),
            fld('sample_metadata_ingest_file', 'sample_metadata_ingest_file'),
            fld('sample_metadata_update_history', 'sample_metadata_update_history', coerce=ingest_utils.get_date_isoformat),
            fld('ammonium_nitrogen_wt', 'ammonium_nitrogen_wt'),
            fld('ammonium_nitrogen_wt_meth', 'ammonium_nitrogen_wt_meth'),
            fld('biotic_relationship', 'biotic_relationship'),
            fld('boron_hot_cacl2', 'boron_hot_cacl2'),
            fld('boron_hot_cacl2_meth', 'boron_hot_cacl2_meth'),
            fld('clay', 'clay'),
            fld('clay_meth', 'clay_meth'),
            fld('coarse_sand', 'coarse_sand'),
            fld('coarse_sand_meth', 'coarse_sand_meth'),
            fld('color', 'color'),
            fld('color_meth', 'color_meth'),
            fld('conductivity', 'conductivity'),
            fld('conductivity_meth', 'conductivity_meth'),
            fld('crop_rotation_1yr_since_present', 'crop_rotation_1yr_since_present'),
            fld('crop_rotation_2yrs_since_present', 'crop_rotation_2yrs_since_present'),
            fld('crop_rotation_3yrs_since_present', 'crop_rotation_3yrs_since_present'),
            fld('crop_rotation_4yrs_since_present', 'crop_rotation_4yrs_since_present'),
            fld('crop_rotation_5yrs_since_present', 'crop_rotation_5yrs_since_present'),
            fld('density', 'density'),
            fld('density_meth', 'density_meth'),
            fld('dtpa_copper', 'dtpa_copper'),
            fld('dtpa_copper_meth', 'dtpa_copper_meth'),
            fld('dtpa_iron', 'dtpa_iron'),
            fld('dtpa_iron_meth', 'dtpa_iron_meth'),
            fld('dtpa_manganese', 'dtpa_manganese'),
            fld('dtpa_manganese_meth', 'dtpa_manganese_meth'),
            fld('dtpa_zinc', 'dtpa_zinc'),
            fld('dtpa_zinc_meth', 'dtpa_zinc_meth'),
            fld('exc_aluminium', 'exc_aluminium'),
            fld('exc_aluminium_meth', 'exc_aluminium_meth'),
            fld('exc_calcium', 'exc_calcium'),
            fld('exc_calcium_meth', 'exc_calcium_meth'),
            fld('exc_magnesium', 'exc_magnesium'),
            fld('exc_magnesium_meth', 'exc_magnesium_meth'),
            fld('exc_potassium', 'exc_potassium'),
            fld('exc_potassium_meth', 'exc_potassium_meth'),
            fld('exc_sodium', 'exc_sodium'),
            fld('exc_sodium_meth', 'exc_sodium_meth'),
            fld('fine_sand', 'fine_sand'),
            fld('fine_sand_meth', 'fine_sand_meth'),
            fld('gravel', 'gravel'),
            fld('gravel_meth', 'gravel_meth'),
            fld('microbial_biomass', 'microbial_biomass'),
            fld('microbial_biomass_meth', 'microbial_biomass_meth'),
            fld('nitrate_nitrogen', 'nitrate_nitrogen'),
            fld('nitrate_nitrogen_meth', 'nitrate_nitrogen_meth'),
            fld('organic_carbon', 'organic_carbon'),
            fld('organic_carbon_meth', 'organic_carbon_meth'),
            fld('ph', 'ph'),
            fld('ph_meth', 'ph_meth'),
            fld('ph_solid_h2o', 'ph_solid_h2o'),
            fld('ph_solid_h2o_meth', 'ph_solid_h2o_meth'),
            fld('phosphorus_colwell', 'phosphorus_colwell'),
            fld('phosphorus_colwell_meth', 'phosphorus_colwell_meth'),
            fld('potassium_colwell', 'potassium_colwell'),
            fld('potassium_colwell_meth', 'potassium_colwell_meth'),
            fld('samp_collect_device', 'samp_collect_device'),
            fld('samp_mat_process', 'samp_mat_process'),
            fld('sand', 'sand'),
            fld('sand_meth', 'sand_meth'),
            fld('silt', 'silt'),
            fld('silt_meth', 'silt_meth'),
            fld('slope_gradient', 'slope_gradient'),
            fld('sulphur', 'sulphur'),
            fld('sulphur_meth', 'sulphur_meth'),
            fld('texture', 'texture'),
            fld('texture_meth', 'texture_meth'),
            fld('total_nitrogen', 'total_nitrogen'),
            fld('total_nitrogen_meth', 'total_nitrogen_meth'),
            fld('water_content', 'water_content'),
            fld('water_content_soil_meth', 'water_content_soil_meth'),
            fld('dna_concentration', 'dna_concentration'),
            fld('dna_concentration_method', 'dna_concentration_method'),
]

"""
