import re
from collections import defaultdict
from glob import glob

import pandas

from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    ExcelWrapper,
    FieldDefinition,
    make_field_definition as fld,
)
from ...util import one

CHEM_MIN_SENTINAL_VALUE = 0.0001


class NotInVocabulary(Exception):
    pass


class AustralianMicrobiomeSampleContextualSQLite:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/amd/metadata/sqlitecontextual/2020-08-18/"
    ]
    metadata_patterns = [re.compile(r"^.*\.db$")]
    name = "amd-samplecontextualsqlite"
    field_specs = {
        "Sqlite": [
            fld("sample_id", "sample_id", coerce=ingest_utils.extract_ands_id),
            fld(
                "utc_date_sampled",
                "utc_date_sampled_yyyymmdd",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "utc_time_sampled",
                "utc_time_sampled_hhmmss",
                coerce=ingest_utils.get_time,
            ),
            fld("longitude_decimal_degrees", "longitude_decimal_degrees"),
            fld("latitude_decimal_degrees", "latitude_decimal_degrees"),
            fld("geo_loc_country_subregion", "geo_loc_country_subregion"),
            fld("sample_site_location_description", "sample_site_location_description"),
            fld("sample_submitter", "sample_submitter"),
            fld("sample_attribution", "sample_attribution"),
            fld("funding_agency", "funding_agency"),
            fld("sample_collection_device_method", "sample_collection_device_method"),
            fld("sample_material_processing", "sample_material_processing"),
            fld(
                "sample_material_processing_method", "sample_material_processing_method"
            ),
            fld("sample_storage_method", "sample_storage_method"),
            fld("environment_controlled_vocab_a", "environment_controlled_vocab_a"),
            fld("env_material_control_vocab_0", "env_material_control_vocab_0"),
            fld(
                "broad_land_use_major_head_control_vocab_2",
                "broad_land_use_major_head_control_vocab_2",
            ),
            fld(
                "detailed_land_use_sub_head_control_vocab_2",
                "detailed_land_use_sub_head_control_vocab_2",
            ),
            fld(
                "general_env_feature_control_vocab_3",
                "general_env_feature_control_vocab_3",
            ),
            fld("vegetation_type", "vegetation_type"),
            fld("notes", "notes"),
            fld(
                "depth_lower",
                "depth_lower_m",
                units="m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "depth_upper",
                "depth_upper_m",
                units="m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("acid_volatile_sulphides", "acid_volatile_sulphides"),
            fld("acid_volatile_sulphides_method", "acid_volatile_sulphides_method"),
            fld("agrochemical_additions", "agrochemical_additions"),
            fld(
                "allo",
                "allo_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("allo_method", "allo_method"),
            fld(
                "alpha_beta_car",
                "alpha_beta_car_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("alpha_beta_car_method", "alpha_beta_car_method"),
            fld("ammonium_nitrogen", "ammonium_nitrogen"),
            fld("ammonium_nitrogen_method", "ammonium_nitrogen_method"),
            fld(
                "ammonium_nitrogen_mg_per_kg",
                "ammonium_nitrogen_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "ammonium_nitrogen_mg_per_kg_method",
                "ammonium_nitrogen_mg_per_kg_method",
            ),
            fld(
                "ammonium_nitrogen_mg_per_l",
                "ammonium_nitrogen_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "ammonium_nitrogen_mg_per_l_method", "ammonium_nitrogen_mg_per_l_method"
            ),
            fld(
                "ammonium",
                "ammonium_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("ammonium_μmol_per_l_method", "ammonium_μmol_per_l_method"),
            fld(
                "anth",
                "anth_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("anth_method", "anth_method"),
            fld("antimony", "antimony"),
            fld("antimony_method", "antimony_method"),
            fld(
                "arsenic",
                "arsenic_µg_per_kg",
                units="µg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("arsenic_method", "arsenic_method"),
            fld(
                "asta",
                "asta_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("asta_method", "asta_method"),
            fld(
                "australian_soil_classification_control_vocab_6",
                "australian_soil_classification_control_vocab_6",
            ),
            fld(
                "average_host_abundance",
                "average_host_abundance_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("barium", "barium"),
            fld("barium_method", "barium_method"),
            fld("base_amplicon_linkage", "base_amplicon_linkage"),
            fld(
                "beta_beta_car",
                "beta_beta_car_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("beta_beta_car_method", "beta_beta_car_method"),
            fld(
                "beta_epi_car",
                "beta_epi_car_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("beta_epi_car_method", "beta_epi_car_method"),
            fld(
                "bleaching",
                "bleaching_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "boron_hot_cacl2",
                "boron_hot_cacl2_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("boron_hot_cacl2_method", "boron_hot_cacl2_method"),
            fld("bottle_number", "bottle_number"),
            fld("bottom_depth", "bottom_depth"),
            fld(
                "bulk_density_g_per_cm3_or",
                "bulk_density_g_per_cm3_or_kg_per_m3",
                units="kg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("bulk_density_method", "bulk_density_method"),
            fld(
                "but_fuco",
                "but_fuco_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("but_fuco_method", "but_fuco_method"),
            fld(
                "cadmium",
                "cadmium_µg_per_kg",
                units="µg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cadmium_method", "cadmium_method"),
            fld(
                "cantha",
                "cantha_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cantha_method", "cantha_method"),
            fld(
                "carbonate_bicarbonate",
                "carbonate_bicarbonate_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("carbonate_bicarbonate_method", "carbonate_bicarbonate_method"),
            fld("cation_exchange_capacity", "cation_exchange_capacity"),
            fld("cation_exchange_capacity_method", "cation_exchange_capacity_method"),
            fld("cerium", "cerium"),
            fld("cerium_method", "cerium_method"),
            fld("cesium", "cesium"),
            fld("cesium_method", "cesium_method"),
            fld(
                "chloride",
                "chloride_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chloride_method", "chloride_method"),
            fld(
                "chlorophyll_a",
                "chlorophyll_a_μg_per_l",
                units="µg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chlorophyll_a_method", "chlorophyll_a_method"),
            fld(
                "chlorophyll_ctd",
                "chlorophyll_ctd_ug_per_l",
                units="ug/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chlorophyll_ctd_method", "chlorophyll_ctd_method"),
            fld(
                "chromium",
                "chromium_µg_per_kg",
                units="µg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chromium_method", "chromium_method"),
            fld("citation", "citation"),
            fld(
                "clay", "clay_percent", units="%", coerce=ingest_utils.get_clean_number
            ),
            fld("clay_method", "clay_method"),
            fld(
                "coarse_sand",
                "coarse_sand_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("coarse_sand_method", "coarse_sand_method"),
            fld("coastal_id", "coastal_id"),
            fld(
                "cobalt",
                "cobalt_µg_per_kg",
                units="µg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cobalt_method", "cobalt_method"),
            fld("color_control_vocab_10", "color_control_vocab_10"),
            fld(
                "conductivity_ds_per_m",
                "conductivity_ds_per_m",
                units="ds/m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("conductivity_ds_per_m_method", "conductivity_ds_per_m_method"),
            fld(
                "conductivity",
                "conductivity_s_per_m",
                units="s/m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("conductivity_s_per_m_method", "conductivity_s_per_m_method"),
            fld(
                "contextual_data_submission_date",
                "contextual_data_submission_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "cphl_a",
                "cphl_a_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cphl_a_method", "cphl_a_method"),
            fld(
                "cphl_b",
                "cphl_b_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cphl_b_method", "cphl_b_method"),
            fld(
                "cphl_c1c2",
                "cphl_c1c2_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cphl_c1c2_method", "cphl_c1c2_method"),
            fld(
                "cphl_c1",
                "cphl_c1_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cphl_c1_method", "cphl_c1_method"),
            fld(
                "cphl_c2",
                "cphl_c2_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cphl_c2_method", "cphl_c2_method"),
            fld(
                "cphl_c3",
                "cphl_c3_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cphl_c3_method", "cphl_c3_method"),
            fld(
                "cphlide_a",
                "cphlide_a_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cphlide_a_method", "cphlide_a_method"),
            fld("crop_rotation_1yr_since_present", "crop_rotation_1yr_since_present"),
            fld("crop_rotation_2yrs_since_present", "crop_rotation_2yrs_since_present"),
            fld("crop_rotation_3yrs_since_present", "crop_rotation_3yrs_since_present"),
            fld("crop_rotation_4yrs_since_present", "crop_rotation_4yrs_since_present"),
            fld("crop_rotation_5yrs_since_present", "crop_rotation_5yrs_since_present"),
            fld(
                "current_land_use_controlled_vocab_2",
                "current_land_use_controlled_vocab_2",
            ),
            fld(
                "date_since_change_in_land_use",
                "date_since_change_in_land_use",
                coerce=ingest_utils.get_year,
            ),
            fld(
                "density_ctd_density",
                "density_ctd_density_kg_per_m3",
                units="kg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("density_method", "density_method"),
            fld("description", "description"),
            fld(
                "diadchr",
                "diadchr_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("diadchr_method", "diadchr_method"),
            fld(
                "diadino",
                "diadino_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("diadino_method", "diadino_method"),
            fld(
                "diato",
                "diato_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("diato_method", "diato_method"),
            fld(
                "dino",
                "dino_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dino_method", "dino_method"),
            fld(
                "dtpa_copper",
                "dtpa_copper_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dtpa_copper_method", "dtpa_copper_method"),
            fld(
                "dtpa_iron",
                "dtpa_iron_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dtpa_iron_method", "dtpa_iron_method"),
            fld(
                "dtpa_manganese",
                "dtpa_manganese_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dtpa_manganese_method", "dtpa_manganese_method"),
            fld(
                "dtpa_zinc",
                "dtpa_zinc_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dtpa_zinc_method", "dtpa_zinc_method"),
            fld(
                "dv_cphl_a_and_cphl_a",
                "dv_cphl_a_and_cphl_a_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dv_cphl_a_and_cphl_a_method", "dv_cphl_a_and_cphl_a_method"),
            fld(
                "dv_cphl_a",
                "dv_cphl_a_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dv_cphl_a_method", "dv_cphl_a_method"),
            fld(
                "dv_cphl_b_and_cphl_b",
                "dv_cphl_b_and_cphl_b_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dv_cphl_b_and_cphl_b_method", "dv_cphl_b_and_cphl_b_method"),
            fld(
                "dv_cphl_b",
                "dv_cphl_b_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dv_cphl_b_method", "dv_cphl_b_method"),
            fld("dysprosium", "dysprosium"),
            fld("dysprosium_method", "dysprosium_method"),
            fld(
                "echin",
                "echin_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("echin_method", "echin_method"),
            fld(
                "elevation",
                "elevation_m",
                units="m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("environment", "environment"),
            fld("erbium", "erbium"),
            fld("erbium_method", "erbium_method"),
            fld("europium", "europium"),
            fld("europium_method", "europium_method"),
            fld(
                "exc_aluminium",
                "exc_aluminium_meq_per_100g",
                units="meq/100g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("exc_aluminium_method", "exc_aluminium_method"),
            fld(
                "exc_calcium",
                "exc_calcium_meq_per_100g",
                units="meq/100g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("exc_calcium_method", "exc_calcium_method"),
            fld(
                "exc_magnesium",
                "exc_magnesium_meq_per_100g",
                units="meq/100g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("exc_magnesium_method", "exc_magnesium_method"),
            fld(
                "exc_potassium",
                "exc_potassium_meq_per_100g",
                units="meq/100g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("exc_potassium_method", "exc_potassium_method"),
            fld(
                "exc_sodium",
                "exc_sodium_meq_per_100g",
                units="meq/100g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("exc_sodium_method", "exc_sodium_method"),
            fld("extreme_events", "extreme_events"),
            fld("facility", "facility"),
            fld(
                "fao_soil_classification_control_vocab_7",
                "fao_soil_classification_control_vocab_7",
            ),
            fld(
                "fine_sand",
                "fine_sand_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("fine_sand_method", "fine_sand_method"),
            fld(
                "fine_sediment",
                "fine_sediment_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("fine_sediment_method", "fine_sediment_method"),
            fld("fire", "fire"),
            fld("fire_intensity_if_known", "fire_intensity_if_known"),
            fld("flooding", "flooding"),
            fld("fluorescence_au", "fluorescence_au"),
            fld("fluorescence_method", "fluorescence_method"),
            fld("fouling_organisms", "fouling_organisms"),
            fld(
                "fouling",
                "fouling_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "fuco",
                "fuco_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("fuco_method", "fuco_method"),
            fld("gadolinium", "gadolinium"),
            fld("gadolinium_method", "gadolinium_method"),
            fld("gallium", "gallium"),
            fld("gallium_method", "gallium_method"),
            fld("geospatial_coverage", "geospatial_coverage"),
            fld("germanium", "germanium"),
            fld("germanium_method", "germanium_method"),
            fld("gold", "gold"),
            fld("gold_method", "gold_method"),
            fld(
                "gravel",
                "gravel_percent_gt2_mm",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("gravel_percent_method", "gravel_percent_method"),
            fld("grazing_number", "grazing_number"),
            fld(
                "grazing",
                "grazing_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "gyro",
                "gyro_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("gyro_method", "gyro_method"),
            fld("hafnium", "hafnium"),
            fld("hafnium_method", "hafnium_method"),
            fld(
                "hex_fuco",
                "hex_fuco_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("hex_fuco_method", "hex_fuco_method"),
            fld("holmium", "holmium"),
            fld("holmium_method", "holmium_method"),
            fld("horizon_control_vocab_1", "horizon_control_vocab_1"),
            fld(
                "host_abundance_mean",
                "host_abundance_mean_individuals_per_m2",
                units="individuals/m2",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "host_abundance",
                "host_abundance_individuals_per_m2",
                units="individuals/m2",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "host_abundance_seaweed_mean",
                "host_abundance_seaweed_mean_ind_per_m2",
                units="ind/m2",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "host_associated_microbiome_zone_see_vocab_d",
                "host_associated_microbiome_zone_see_vocab_d",
            ),
            fld("host_species", "host_species"),
            fld("host_state", "host_state"),
            fld("host_type_see_vocab_c", "host_type_see_vocab_c"),
            fld(
                "icp_te_boron",
                "icp_te_boron_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_boron_method", "icp_te_boron_method"),
            fld(
                "icp_te_calcium",
                "icp_te_calcium_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_calcium_method", "icp_te_calcium_method"),
            fld(
                "icp_te_copper",
                "icp_te_copper_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_copper_method", "icp_te_copper_method"),
            fld(
                "icp_te_iron",
                "icp_te_iron_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_iron_method", "icp_te_iron_method"),
            fld(
                "icp_te_magnesium",
                "icp_te_magnesium_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_magnesium_method", "icp_te_magnesium_method"),
            fld(
                "icp_te_manganese",
                "icp_te_manganese_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_manganese_method", "icp_te_manganese_method"),
            fld(
                "icp_te_phosphorus",
                "icp_te_phosphorus_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_phosphorus_method", "icp_te_phosphorus_method"),
            fld(
                "icp_te_potassium",
                "icp_te_potassium_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_potassium_method", "icp_te_potassium_method"),
            fld(
                "icp_te_sodium",
                "icp_te_sodium_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_sodium_method", "icp_te_sodium_method"),
            fld(
                "icp_te_sulfur",
                "icp_te_sulfur_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_sulfur_method", "icp_te_sulfur_method"),
            fld(
                "icp_te_zinc",
                "icp_te_zinc_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_zinc_method", "icp_te_zinc_method"),
            fld(
                "immediate_previous_land_use_control_vocab_2",
                "immediate_previous_land_use_control_vocab_2",
            ),
            fld("imos_site_code", "imos_site_code"),
            fld("information", "information"),
            fld(
                "inorganic_fraction",
                "inorganic_fraction_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("inorganic_fraction_method", "inorganic_fraction_method"),
            fld("iridium", "iridium"),
            fld("iridium_method", "iridium_method"),
            fld(
                "keto_hex_fuco",
                "keto_hex_fuco_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("keto_hex_fuco_method", "keto_hex_fuco_method"),
            fld("lanthanum", "lanthanum"),
            fld("lanthanum_method", "lanthanum_method"),
            fld("lat_raw", "lat_raw"),
            fld(
                "lead",
                "lead_µg_per_kg",
                units="µg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("lead_method", "lead_method"),
            fld("length_cm", "length_cm"),
            fld("light_intensity_lux", "light_intensity_lux"),
            fld("light_intensity_lux_method", "light_intensity_lux_method"),
            fld(
                "light_intensity_meadow_µmol_per",
                "light_intensity_meadow_µmol_per_m2_per_s1",
                units="m2/s1",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("light_intensity_meadow_method", "light_intensity_meadow_method"),
            fld(
                "light_intensity_µmol_m2_s1_bottom", "light_intensity_µmol_m2_s1_bottom"
            ),
            fld("light_intensity_bottom_method", "light_intensity_bottom_method"),
            fld(
                "light_intensity_µmol_m2_s1_surface",
                "light_intensity_µmol_m2_s1_surface",
            ),
            fld("light_intensity_surface_method", "light_intensity_surface_method"),
            fld("lon_raw", "lon_raw"),
            fld("lutetium", "lutetium"),
            fld("lutetium_method", "lutetium_method"),
            fld(
                "lut",
                "lut_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("lut_method", "lut_method"),
            fld(
                "lyco",
                "lyco_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("lyco_method", "lyco_method"),
            fld("metals", "metals"),
            fld("metals_method", "metals_method"),
            fld(
                "method_of_australian_soil_classification",
                "method_of_australian_soil_classification",
            ),
            fld(
                "method_of_fao_soil_classification", "method_of_fao_soil_classification"
            ),
            fld(
                "mg_dvp",
                "mg_dvp_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("mg_dvp_method", "mg_dvp_method"),
            fld(
                "microbial_abundance",
                "microbial_abundance_cells_per_ml",
                units="cells/ml",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("microbial_abundance_method", "microbial_abundance_method"),
            fld("microbial_biomass", "microbial_biomass"),
            fld("microbial_biomass_method", "microbial_biomass_method"),
            fld("molybdenum", "molybdenum"),
            fld("molybdenum_method", "molybdenum_method"),
            fld("mud", "mud_percent", units="%", coerce=ingest_utils.get_clean_number),
            fld("mud_method", "mud_method"),
            fld("neodymium", "neodymium"),
            fld("neodymium_method", "neodymium_method"),
            fld(
                "neo",
                "neo_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("neo_method", "neo_method"),
            fld("nickel", "nickel"),
            fld("nickel_method", "nickel_method"),
            fld("niobium_columbium", "niobium_columbium"),
            fld("niobium_columbium_method", "niobium_columbium_method"),
            fld(
                "nitrate_nitrite",
                "nitrate_nitrite_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("nitrate_nitrite_method", "nitrate_nitrite_method"),
            fld(
                "nitrate_nitrogen",
                "nitrate_nitrogen_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "nitrate_nitrogen_mg_per_kg_method", "nitrate_nitrogen_mg_per_kg_method"
            ),
            fld(
                "nitrite",
                "nitrite_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("nitrite_method", "nitrite_method"),
            fld(
                "no2",
                "no2_µmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("no2_method", "no2_method"),
            fld(
                "npic",
                "npic_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("npic_method", "npic_method"),
            fld(
                "npoc",
                "npoc_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("npoc_method", "npoc_method"),
            fld("nrs_sample_code", "nrs_sample_code"),
            fld("nrs_trip_code", "nrs_trip_code"),
            fld("operation_cast_id", "operation_cast_id"),
            fld(
                "organic_carbon",
                "organic_carbon_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("organic_carbon_percent_method", "organic_carbon_percent_method"),
            fld(
                "organic_fraction",
                "organic_fraction_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("organic_fraction_method", "organic_fraction_method"),
            fld("organic_matter_content_loi", "organic_matter_content_loi"),
            fld("organic_matter_content_method", "organic_matter_content_method"),
            fld("osmium", "osmium"),
            fld("osmium_method", "osmium_method"),
            fld("oxygen_ml_per_l_ctd", "oxygen_ml_per_l_ctd"),
            fld("oxygen_ml_per_l_method", "oxygen_ml_per_l_method"),
            fld(
                "oxygen_ctd",
                "oxygen_μmol_per_kg_ctd",
                units="µmol_per_kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("oxygen_μmol_per_kg_ctd_method", "oxygen_μmol_per_kg_ctd_methods"),
            fld(
                "oxygen",
                "oxygen_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("oxygen_μmol_per_l_method", "oxygen_μmol_per_l_method"),
            fld("palladium", "palladium"),
            fld("palladium_method", "palladium_method"),
            fld("pam_fluorometer_measurement", "pam_fluorometer_measurement"),
            fld(
                "pam_fluorometer_measurement_method",
                "pam_fluorometer_measurement_method",
            ),
            fld(
                "perid",
                "perid_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("perid_method", "perid_method"),
            fld("ph_aqueous", "ph_aqueous"),
            fld("ph_aqueous_method", "ph_aqueous_method"),
            fld(
                "phide_a",
                "phide_a_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phide_a_method", "phide_a_method"),
            fld(
                "phosphate",
                "phosphate_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phosphate_method", "phosphate_method"),
            fld(
                "phosphorus_colwell",
                "phosphorus_colwell_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phosphorus_colwell_method", "phosphorus_colwell_method"),
            fld("ph_solid_cacl2", "ph_solid_cacl2"),
            fld("ph_solid_cacl2_method", "ph_solid_cacl2_method"),
            fld("ph_solid_h2o", "ph_solid_h2o"),
            fld("ph_solid_h2o_method", "ph_solid_h2o_method"),
            fld(
                "phytin_a",
                "phytin_a_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phytin_a_method", "phytin_a_method"),
            fld(
                "phytin_b",
                "phytin_b_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phytin_b_method", "phytin_b_method"),
            fld("pigments", "pigments"),
            fld("pigments_methods", "pigments_methods"),
            fld("platinum", "platinum"),
            fld("platinum_method", "platinum_method"),
            fld(
                "pn",
                "pn_µmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pn_method", "pn_method"),
            fld(
                "poc",
                "poc_µmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("poc_method", "poc_method"),
            fld("porewater_ph", "porewater_ph"),
            fld("porewater_ph_method", "porewater_ph_method"),
            fld(
                "potassium_colwell",
                "potassium_colwell_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("potassium_colwell_method", "potassium_colwell_method"),
            fld("praseodymium", "praseodymium"),
            fld("praseodymium_method", "praseodymium_method"),
            fld(
                "pras",
                "pras_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pras_method", "pras_method"),
            fld("pres_rel_dbar", "pres_rel_dbar"),
            fld("pres_rel_method", "pres_rel_method"),
            fld("profile_position_control_vocab_5", "profile_position_control_vocab_5"),
            fld(
                "pyrophide_a",
                "pyrophide_a_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pyrophide_a_method", "pyrophide_a_method"),
            fld(
                "pyrophytin_a",
                "pyrophytin_a_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pyrophytin_a_method", "pyrophytin_a_method"),
            fld("rhodium", "rhodium"),
            fld("rhodium_method", "rhodium_method"),
            fld("rubidium", "rubidium"),
            fld("rubidium_method", "rubidium_method"),
            fld("ruthenium", "ruthenium"),
            fld("ruthenium_method", "ruthenium_method"),
            fld("salinity_ctd_psu", "salinity_ctd_psu"),
            fld("salinity_ctd_method", "salinity_ctd_method"),
            fld("salinity_lab_psu", "salinity_lab_psu"),
            fld("salinity_lab_method", "salinity_lab_method"),
            fld("samarium", "samarium"),
            fld("samarium_method", "samarium_method"),
            fld("sample_type", "sample_type"),
            fld(
                "sand", "sand_percent", units="%", coerce=ingest_utils.get_clean_number
            ),
            fld("sand_method", "sand_method"),
            fld("scandium", "scandium"),
            fld("scandium_method", "scandium_method"),
            fld(
                "secchi_depth",
                "secchi_depth_m",
                units="m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("secchi_depth_method", "secchi_depth_method"),
            fld(
                "sedimentation_rate",
                "sedimentation_rate_g_per_cm2_per_yr",
                units="g/(cm2 x yr)",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sedimentation_rate_method", "sedimentation_rate_method"),
            fld(
                "sediment_porewater_h4sio4",
                "sediment_porewater_h4sio4_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_h4sio4_method", "sediment_porewater_h4sio4_method"),
            fld(
                "sediment_porewater_nh4",
                "sediment_porewater_nh4_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_nh4_method", "sediment_porewater_nh4_method"),
            fld(
                "sediment_porewater_no2",
                "sediment_porewater_no2_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_no2_method", "sediment_porewater_no2_method"),
            fld(
                "sediment_porewater_no3",
                "sediment_porewater_no3_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_no3_method", "sediment_porewater_no3_method"),
            fld(
                "sediment_porewater_po43",
                "sediment_porewater_po43_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_po43_method", "sediment_porewater_po43_method"),
            fld("selenium", "selenium"),
            fld("selenium_method", "selenium_method"),
            fld(
                "silicate",
                "silicate_μmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("silicate_method", "silicate_method"),
            fld(
                "silt", "silt_percent", units="%", coerce=ingest_utils.get_clean_number
            ),
            fld("silt_method", "silt_method"),
            fld("silver", "silver"),
            fld("silver_method", "silver_method"),
            fld(
                "sio2",
                "sio2_µmol_per_l",
                units="µmol/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sio2_method", "sio2_method"),
            fld(
                "slope_aspect_direction_or_degrees",
                "slope_aspect_direction_or_degrees_eg_nw_or_315",
                units="direction or degrees",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "slope",
                "slope_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "soil_moisture",
                "soil_moisture_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("soil_moisture_method", "soil_moisture_method"),
            fld("strontium", "strontium"),
            fld("strontium_method", "strontium_method"),
            fld(
                "sulphur",
                "sulphur_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sulphur_method", "sulphur_method"),
            fld("tags", "tags"),
            fld("tantalum", "tantalum"),
            fld("tantalum_method", "tantalum_method"),
            fld(
                "temperature",
                "temperature_deg_c",
                units="its-90 deg c",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("temperature_method", "temperature_method"),
            fld(
                "temperature_ctd",
                "temperature_ctd_deg_c",
                units="its-90 deg c",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("temperature_ctd_method", "temperature_ctd_method"),
            fld("terbium", "terbium"),
            fld("terbium_method", "terbium_method"),
            fld("texture", "texture"),
            fld("texture_method", "texture_method"),
            fld("thorium", "thorium"),
            fld("thorium_method", "thorium_method"),
            fld("thulium", "thulium"),
            fld("thulium_method", "thulium_method"),
            fld("tillage_control_vocab_9", "tillage_control_vocab_9"),
            fld("time_sampled", "time_sampled", coerce=ingest_utils.get_time),
            fld("tin", "tin"),
            fld("tin_method", "tin_method"),
            fld("toc", "toc"),
            fld("toc_method", "toc_method"),
            fld(
                "total_alkalinity",
                "total_alkalinity_μmol_per_kg",
                units="µmol/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("total_alkalinity_method", "total_alkalinity_method"),
            fld(
                "total_carbon",
                "total_carbon_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("total_carbon_percent_method", "total_carbon_percent_method"),
            fld(
                "total_co2",
                "total_co2_μmol_per_kg",
                units="µmol/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("total_co2_method", "total_co2_method"),
            fld(
                "total_inorganic_carbon",
                "total_inorganic_carbon_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "total_inorganic_carbon_percent_method",
                "total_inorganic_carbon_percent_method",
            ),
            fld("total_nitrogen", "total_nitrogen"),
            fld("total_nitrogen_method", "total_nitrogen_method"),
            fld(
                "total_phosphorous",
                "total_phosphorous_mg_per_kg",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "total_phosphorous_mg_per_kg_method",
                "total_phosphorous_mg_per_kg_method",
            ),
            fld(
                "total_phosphorous_percent",
                "total_phosphorous_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("total_phosphorous_method", "total_phosphorous_method"),
            fld("touching_organisms", "touching_organisms"),
            fld(
                "transmittance",
                "transmittance_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("transmittance_method", "transmittance_method"),
            fld(
                "tss",
                "tss_mg_per_l",
                units="mg/l",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("tss_method", "tss_method"),
            fld("tungsten_or_wolfram", "tungsten_or_wolfram"),
            fld("tungsten_or_wolfram_method", "tungsten_or_wolfram_method"),
            fld(
                "turbidity",
                "turbidity_ntu",
                units="ntu",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("turbidity_ntu_method", "turbidity_ntu_method"),
            fld(
                "turbidity_ctd",
                "turbidity_ctd_nephelometric_turbidity_units",
                units="nephelometric_turbidity_units",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("turbidity_ctd_method", "turbidity_ctd_method"),
            fld("uranium", "uranium"),
            fld("uranium_method", "uranium_method"),
            fld("vanadium", "vanadium"),
            fld("vanadium_method", "vanadium_method"),
            fld(
                "vegetation_dom_grasses",
                "vegetation_dom_grasses_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "vegetation_dom_shrubs",
                "vegetation_dom_shrubs_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "vegetation_dom_trees",
                "vegetation_dom_trees_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "vegetation_total_basal_area",
                "vegetation_total_basal_area_m2_in_0p25ha",
                units="m2_in_0p25ha",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "vegetation_total_cover",
                "vegetation_total_cover_percent",
                units="%",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("vegetation_type_descriptive", "vegetation_type_descriptive"),
            fld(
                "viola",
                "viola_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("viola_method", "viola_method"),
            fld("voyage_code", "voyage_code"),
            fld("voyage_survey_link", "voyage_survey_link"),
            fld(
                "water_depth",
                "water_depth_m",
                units="m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("water_holding_capacity", "water_holding_capacity"),
            fld("water_holding_capacity_method", "water_holding_capacity_method"),
            fld("ytterbium", "ytterbium"),
            fld("ytterbium_method", "ytterbium_method"),
            fld("yttrium", "yttrium"),
            fld("yttrium_method", "yttrium_method"),
            fld(
                "zea",
                "zea_mg_per_m3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("zea_method", "zea_method"),
            fld("zirconium", "zirconium"),
            fld("zirconium_method", "zirconium_method"),
        ],
    }

    def __init__(self, logger, path):
        self._logger = logger
        db_path = one(glob(path + "/*.db"))
        # needs to be in list of dictionaries organised by library_id
        data_frames = self.test_for_sqlite(db_path)
        fo = tempfile.NamedTemporaryFile(suffix=".xlsx")
        self._logger.info("Have temp file: {}".format(fo.name))
        self.dataframe_to_excel_file(data_frames, fo.name)
        # fo.name
        # fo.close()
        # xlsx_path = one(glob(path + "/*.xlsx"))
        self.environment_ontology_errors = defaultdict(set)
        self.sample_metadata = self._package_metadata(self._read_metadata(fo.name))
        fo.close()
        self._logger.info("context file processing completed.")

    def dataframe_to_excel_file(self, df, fname):
        writer = pandas.ExcelWriter(fname)
        df.to_excel(writer, sheet_name="Sqlite")
        writer.save()
        self._logger.info("Excel file written.")

    def test_for_sqlite(self, db_path):
        import sqlite3 as lite
        import sys

        con = None

        try:
            con = lite.connect(db_path)

            # basic test for working db
            cur = con.cursor()
            cur.execute("SELECT SQLITE_VERSION()")
            data = cur.fetchone()
            self._logger.info("SQLite version: %s" % data)
            df = pandas.read_sql_query("SELECT * FROM AM_metadata", con)
            return df
        except lite.Error as e:
            self._logger.error("Error %s:" % e.args[0])
            sys.exit(1)
        finally:
            if con:
                con.close()

    @classmethod
    def units_for_fields(cls):
        r = {}
        for sheet_name, fields in cls.field_specs.items():
            for field in fields:
                if not isinstance(field, FieldDefinition):
                    continue
                if field.attribute in r and r[field.attribute] != field.units:
                    raise Exception("units inconsistent for field: {}", field.attribute)
                r[field.attribute] = field.units
        return r

    def sample_ids(self):
        return list(self.sample_metadata.keys())

    def get(self, sample_id):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        self._logger.warning(
            "no %s metadata available for: %s" % (type(self).__name__, repr(sample_id))
        )
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.sample_id is None:
                continue
            if row.sample_id in sample_metadata:
                raise Exception(
                    "Metadata invalid, duplicate sample ID {} in row {}".format(
                        row.sample_id, row
                    )
                )
            assert row.sample_id not in sample_metadata
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                val = getattr(row, field)
                if field != "sample_id":
                    row_meta[field] = val
        return sample_metadata

    @staticmethod
    def environment_for_sheet(sheet_name):
        return "Soil" if sheet_name == "Soil" else "Marine"

    def _read_metadata(self, metadata_path):
        rows = []
        for sheet_name, field_spec in sorted(self.field_specs.items()):
            wrapper = ExcelWrapper(
                self._logger,
                field_spec,
                metadata_path,
                sheet_name=sheet_name,
                header_length=1,
                column_name_row_index=0,
                suggest_template=True,
                additional_context={},
            )
            for error in wrapper.get_errors():
                self._logger.error(error)
            rows += wrapper.get_all()
        return rows

    def filename_metadata(self, *args, **kwargs):
        return {}
