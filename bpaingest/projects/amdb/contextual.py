import re
from glob import glob

from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    ExcelWrapper,
    FieldDefinition,
    make_field_definition as fld,
)
from ...ncbi import NCBISRAContextual
from ...util import one
from ...abstract import BaseDatasetControlContextual


class NotInVocabulary(Exception):
    pass


def ands_orSAMN(logger, s, silent=False):
    if not re.compile(r"SAMN\d{8}"):
        return ingest_utils.extract_ands_id(logger, s, silent)
    else:
        return s


class AustralianMicrobiomeSampleContextual:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/amd/metadata/contextual/2024-08-08/"
    ]
    metadata_patterns = [re.compile(r"^.*\.xlsx$")]
    name = "amd-samplecontextual"
    sheet_name = "Sample_context"
    source_pattern = "/*.xlsx"
    field_specs = {
        sheet_name: [
            fld("sample_id", "sample_id", coerce=ands_orSAMN),
            fld("source_mat_id", "source_mat_id"),
            fld(
                "utc_date_sampled",
                "utc_date_sampled",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("utc_time_sampled", "utc_time_sampled", coerce=ingest_utils.get_time),
            fld(
                "collection_date",
                "collection_date",
                coerce=ingest_utils.get_date_isoformat_as_datetime,  # Note this can be removed when ckan handles Z tz
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
            fld("sample_integrity_warnings", "sample_integrity_warnings"),
            fld("nucl_acid_ext", "nucl_acid_ext"),
            fld(
                "dna_concentration_submitter",
                "dna_concentration_submitter",
                units="ng/" + "\u03BC" + "L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dna_concentration_submitter_meth", "dna_concentration_submitter_meth"),
            fld(
                "absorbance_260_280_ratio_submitter",
                "absorbance_260_280_ratio_submitter",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "absorbance_260_280_ratio_submitter_meth",
                "absorbance_260_280_ratio_submitter_meth",
            ),
            fld("am_environment", "am_environment"),
            fld(
                "acid_volatile_sulphides",
                "acid_volatile_sulphides",
                units="\u03BC" + "mol/g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("acid_volatile_sulphides_meth", "acid_volatile_sulphides_meth"),
            fld("agrochem_addition", "agrochem_addition"),
            fld(
                "alkalinity",
                "alkalinity",
                units="\u03BC" + "mol/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("alkalinity_meth", "alkalinity_meth"),
            fld("allo", "allo", units="mg/m3", coerce=ingest_utils.get_clean_number),
            fld("allo_meth", "allo_meth"),
            fld(
                "alpha_beta_car",
                "alpha_beta_car",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("alpha_beta_car_meth", "alpha_beta_car_meth"),
            fld(
                "ammonium",
                "ammonium",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("ammonium_meth", "ammonium_meth"),
            fld(
                "ammonium_nitrogen_wt",
                "ammonium_nitrogen_wt",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("ammonium_nitrogen_wt_meth", "ammonium_nitrogen_wt_meth"),
            fld(
                "anth",
                "anth",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("anth_meth", "anth_meth"),
            fld(
                "antimony",
                "antimony",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("antimony_meth", "antimony_meth"),
            fld(
                "arsenic",
                "arsenic",
                units="\u03BC" + "g/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("arsenic_meth", "arsenic_meth"),
            fld(
                "asta",
                "asta",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("asta_meth", "asta_meth"),
            fld(
                "average_host_abundance",
                "average_host_abundance",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("average_host_abundance_meth", "average_host_abundance_meth"),
            fld("barium", "barium", units="ppm", coerce=ingest_utils.get_clean_number),
            fld("barium_meth", "barium_meth"),
            fld(
                "beta_beta_car",
                "beta_beta_car",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("beta_beta_car_meth", "beta_beta_car_meth"),
            fld(
                "beta_epi_car",
                "beta_epi_car",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("beta_epi_car_meth", "beta_epi_car_meth"),
            fld(
                "bicarbonate",
                "bicarbonate",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("bicarbonate_meth", "bicarbonate_meth"),
            fld(
                "bleaching",
                "bleaching",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("bleaching_meth", "bleaching_meth"),
            fld(
                "boron_hot_cacl2",
                "boron_hot_cacl2",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("boron_hot_cacl2_meth", "boron_hot_cacl2_meth"),
            fld(
                "but_fuco",
                "but_fuco",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("but_fuco_meth", "but_fuco_meth"),
            fld(
                "cadmium",
                "cadmium",
                units="\u03BC" + "g/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cadmium_meth", "cadmium_meth"),
            fld(
                "cantha",
                "cantha",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cantha_meth", "cantha_meth"),
            fld(
                "carbonate",
                "carbonate",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("carbonate_meth", "carbonate_meth"),
            fld("cast_id", "cast_id"),
            fld(
                "cation_exchange_capacity",
                "cation_exchange_capacity",
                units="meq/100g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cation_exchange_capacity_meth", "cation_exchange_capacity_meth"),
            fld("cerium", "cerium", units="ppm", coerce=ingest_utils.get_clean_number),
            fld("cerium_meth", "cerium_meth"),
            fld("cesium", "cesium", units="ppm", coerce=ingest_utils.get_clean_number),
            fld("cesium_meth", "cesium_meth"),
            fld(
                "chloride",
                "chloride",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chloride_meth", "chloride_meth"),
            fld(
                "chlorophyll_a",
                "chlorophyll_a",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chlorophyll_a_meth", "chlorophyll_a_meth"),
            fld(
                "chlorophyll_b",
                "chlorophyll_b",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chlorophyll_b_meth", "chlorophyll_b_meth"),
            fld(
                "chlorophyll_c1",
                "chlorophyll_c1",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chlorophyll_c1_meth", "chlorophyll_c1_meth"),
            fld(
                "chlorophyll_c1c2",
                "chlorophyll_c1c2",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chlorophyll_c1c2_meth", "chlorophyll_c1c2_meth"),
            fld(
                "chlorophyll_c2",
                "chlorophyll_c2",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chlorophyll_c2_meth", "chlorophyll_c2_meth"),
            fld(
                "chlorophyll_c3",
                "chlorophyll_c3",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chlorophyll_c3_meth", "chlorophyll_c3_meth"),
            fld(
                "chlorophyll_ctd",
                "chlorophyll_ctd",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chlorophyll_ctd_meth", "chlorophyll_ctd_meth"),
            fld(
                "chromium",
                "chromium",
                units="\u03BC" + "g/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("chromium_meth", "chromium_meth"),
            fld("citation", "citation"),
            fld("clay", "clay", units="%", coerce=ingest_utils.get_percentage),
            fld("clay_meth", "clay_meth"),
            fld("coastal_id", "coastal_id"),
            fld(
                "cobalt",
                "cobalt",
                units="\u03BC" + "g/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cobalt_meth", "cobalt_meth"),
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
                "conductivity_aqueous",
                "conductivity_aqueous",
                units="S/m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("conductivity_aqueous_meth", "conductivity_aqueous_meth"),
            fld(
                "coarse_sand",
                "coarse_sand",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("coarse_sand_meth", "coarse_sand_meth"),
            fld("collection_permit", "collection_permit", optional=True),
            fld(
                "cphlide_a",
                "cphlide_a",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("cphlide_a_meth", "cphlide_a_meth"),
            fld("crop_rotation_1yr_since_present", "crop_rotation_1yr_since_present"),
            fld("crop_rotation_2yrs_since_present", "crop_rotation_2yrs_since_present"),
            fld("crop_rotation_3yrs_since_present", "crop_rotation_3yrs_since_present"),
            fld("crop_rotation_4yrs_since_present", "crop_rotation_4yrs_since_present"),
            fld("crop_rotation_5yrs_since_present", "crop_rotation_5yrs_since_present"),
            fld(
                "date_since_change_in_land_use",
                "date_since_change_in_land_use",
            ),
            fld(
                "days_since_planting",
                "days_since_planting",
                units="days",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "density",
                "density",
                units="kg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("density_meth", "density_meth"),
            fld(
                "diadchr",
                "diadchr",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("diadchr_meth", "diadchr_meth"),
            fld(
                "diadino",
                "diadino",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("diadino_meth", "diadino_meth"),
            fld(
                "diato",
                "diato",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("diato_meth", "diato_meth"),
            fld(
                "dino",
                "dino",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dino_meth", "dino_meth"),
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
            fld(
                "dv_cphl_a_and_cphl_a",
                "dv_cphl_a_and_cphl_a",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dv_cphl_a_and_cphl_a_meth", "dv_cphl_a_and_cphl_a_meth"),
            fld(
                "dv_cphl_a",
                "dv_cphl_a",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dv_cphl_a_meth", "dv_cphl_a_meth"),
            fld(
                "dv_cphl_b_and_cphl_b",
                "dv_cphl_b_and_cphl_b",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dv_cphl_b_and_cphl_b_meth", "dv_cphl_b_and_cphl_b_meth"),
            fld(
                "dv_cphl_b",
                "dv_cphl_b",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dv_cphl_b_meth", "dv_cphl_b_meth"),
            fld(
                "dysprosium",
                "dysprosium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("dysprosium_meth", "dysprosium_meth"),
            fld(
                "echin",
                "echin",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("echin_meth", "echin_meth"),
            fld("elev", "elev", units="m", coerce=ingest_utils.get_clean_number),
            fld("erbium", "erbium", units="ppm", coerce=ingest_utils.get_clean_number),
            fld("erbium_meth", "erbium_meth"),
            fld(
                "europium",
                "europium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("europium_meth", "europium_meth"),
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
            fld("extreme_event", "extreme_event"),
            fld(
                "fine_sand", "fine_sand", units="%", coerce=ingest_utils.get_percentage
            ),
            fld("fine_sand_meth", "fine_sand_meth"),
            fld(
                "fine_sediment",
                "fine_sediment",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("fine_sediment_meth", "fine_sediment_meth"),
            fld("fire", "fire"),
            fld("fire_intensity_if_known", "fire_intensity_if_known"),
            fld("flooding", "flooding"),
            fld("fluor", "fluor", units="AU", coerce=ingest_utils.get_clean_number),
            fld("fluor_meth", "fluor_meth"),
            fld("fouling", "fouling", units="%", coerce=ingest_utils.get_percentage),
            fld("fouling_meth", "fouling_meth"),
            fld("fouling_organisms", "fouling_organisms"),
            fld(
                "fresh_weight",
                "fresh_weight",
                units="g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("fresh_weight_meth", "fresh_weight_meth"),
            fld(
                "fuco",
                "fuco",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("fuco_meth", "fuco_meth"),
            fld(
                "gadolinium",
                "gadolinium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("gadolinium_meth", "gadolinium_meth"),
            fld(
                "gallium", "gallium", units="ppm", coerce=ingest_utils.get_clean_number
            ),
            fld("gallium_meth", "gallium_meth"),
            fld(
                "germanium",
                "germanium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("germanium_meth", "germanium_meth"),
            fld("gold", "gold", units="ppm", coerce=ingest_utils.get_clean_number),
            fld("gold_meth", "gold_meth"),
            fld(
                "gravel",
                "gravel",
                units="%",
            ),
            fld("gravel_meth", "gravel_meth"),
            fld(
                "grazing_number",
                "grazing_number",
                units="individuals/m2",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "grazing",
                "grazing",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("grazing_meth", "grazing_meth"),
            fld("grazing_number_meth", "grazing_number_meth"),
            fld(
                "gyro",
                "gyro",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("gyro_meth", "gyro_meth"),
            fld(
                "hafnium", "hafnium", units="ppm", coerce=ingest_utils.get_clean_number
            ),
            fld("hafnium_meth", "hafnium_meth"),
            fld(
                "hex_fuco",
                "hex_fuco",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("hex_fuco_meth", "hex_fuco_meth"),
            fld(
                "holmium", "holmium", units="ppm", coerce=ingest_utils.get_clean_number
            ),
            fld("holmium_meth", "holmium_meth"),
            fld("horizon", "horizon"),
            fld(
                "host_abundance_mean",
                "host_abundance_mean",
                units="individuals/m2",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "host_abundance",
                "host_abundance",
                units="individuals/m2",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("host_abundance_mean_meth", "host_abundance_mean_meth"),
            fld("host_abundance_meth", "host_abundance_meth"),
            fld(
                "host_abundance_seaweed_mean",
                "host_abundance_seaweed_mean",
                units="individuals/m2",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("host_abundance_seaweed_mean_meth", "host_abundance_seaweed_mean_meth"),
            fld("host_associated_microbiome_zone", "host_associated_microbiome_zone"),
            fld("host_length_meth", "host_length_meth"),
            fld("host_species_variety", "host_species_variety"),
            fld("host_state", "host_state"),
            fld("host_type", "host_type"),
            fld("hyperspectral_analysis", "hyperspectral_analysis", optional=True),
            fld(
                "hyperspectral_analysis_meth",
                "hyperspectral_analysis_meth",
                optional=True,
            ),
            fld(
                "icp_te_boron",
                "icp_te_boron",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_boron_meth", "icp_te_boron_meth"),
            fld(
                "icp_te_calcium",
                "icp_te_calcium",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_calcium_meth", "icp_te_calcium_meth"),
            fld(
                "icp_te_copper",
                "icp_te_copper",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_copper_meth", "icp_te_copper_meth"),
            fld(
                "icp_te_iron",
                "icp_te_iron",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_iron_meth", "icp_te_iron_meth"),
            fld(
                "icp_te_manganese",
                "icp_te_manganese",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_manganese_meth", "icp_te_manganese_meth"),
            fld(
                "icp_te_phosphorus",
                "icp_te_phosphorus",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_phosphorus_meth", "icp_te_phosphorus_meth"),
            fld(
                "icp_te_sulfur",
                "icp_te_sulfur",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_sulfur_meth", "icp_te_sulfur_meth"),
            fld(
                "icp_te_zinc",
                "icp_te_zinc",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("icp_te_zinc_meth", "icp_te_zinc_meth"),
            fld("immediate_previous_land_use", "immediate_previous_land_use"),
            fld("imos_site_code", "imos_site_code"),
            fld("information", "information"),
            fld(
                "inorganic_fraction",
                "inorganic_fraction",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("inorganic_fraction_meth", "inorganic_fraction_meth"),
            fld(
                "iridium", "iridium", units="ppm", coerce=ingest_utils.get_clean_number
            ),
            fld("iridium_meth", "iridium_meth"),
            fld(
                "keto_hex_fuco",
                "keto_hex_fuco",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("keto_hex_fuco_meth", "keto_hex_fuco_meth"),
            fld(
                "lanthanum",
                "lanthanum",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("lanthanum_meth", "lanthanum_meth"),
            fld(
                "lead",
                "lead",
                units="\u03BC" + "g/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("lead_meth", "lead_meth"),
            fld(
                "host_length",
                "host_length",
                units="cm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "light_intensity",
                "light_intensity",
                units="lux",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("light_intensity_meth", "light_intensity_meth"),
            fld(
                "light_intensity_meadow",
                "light_intensity_meadow",
                units="\u03BC" + "mol/m2/s",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("light_intensity_meadow_meth", "light_intensity_meadow_meth"),
            fld(
                "light_intensity_bottom",
                "light_intensity_bottom",
                units="\u03BC" + "mol/m2/s",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("light_intensity_bottom_meth", "light_intensity_bottom_meth"),
            fld(
                "light_intensity_surface",
                "light_intensity_surface",
                units="\u03BC" + "mol/m2/s",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("light_intensity_surface_meth", "light_intensity_surface_meth"),
            fld("local_class", "local_class"),
            fld("local_class_meth", "local_class_meth"),
            fld(
                "lutetium",
                "lutetium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("lutetium_meth", "lutetium_meth"),
            fld(
                "lut",
                "lut",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("lut_meth", "lut_meth"),
            fld(
                "lyco",
                "lyco",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("lyco_meth", "lyco_meth"),
            fld(
                "magnesium",
                "magnesium",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("magnesium_meth", "magnesium_meth"),
            fld(
                "mg_dvp",
                "mg_dvp",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("mg_dvp_meth", "mg_dvp_meth"),
            fld(
                "microbial_abundance",
                "microbial_abundance",
                units="cells/mL",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("microbial_abundance_meth", "microbial_abundance_meth"),
            fld(
                "microbial_biomass",
                "microbial_biomass",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("microbial_biomass_meth", "microbial_biomass_meth"),
            fld(
                "molybdenum",
                "molybdenum",
                units="\u03BC" + "g/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("molybdenum_meth", "molybdenum_meth"),
            fld("mud", "mud", coerce=ingest_utils.get_clean_number),
            fld("mud_meth", "mud_meth"),
            fld(
                "myxo",
                "myxo",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("myxo_meth", "myxo_meth"),
            fld(
                "neodymium",
                "neodymium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("neodymium_meth", "neodymium_meth"),
            fld(
                "neo",
                "neo",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("neo_meth", "neo_meth"),
            fld(
                "nickel",
                "nickel",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("nickel_meth", "nickel_meth"),
            fld(
                "niobium_columbium",
                "niobium_columbium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("niobium_columbium_meth", "niobium_columbium_meth"),
            fld(
                "nitrate",
                "nitrate",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("nitrate_meth", "nitrate_meth"),
            fld(
                "nitrate_nitrite",
                "nitrate_nitrite",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("nitrate_nitrite_meth", "nitrate_nitrite_meth"),
            fld(
                "nitrate_nitrogen",
                "nitrate_nitrogen",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("nitrate_nitrogen_meth", "nitrate_nitrogen_meth"),
            fld(
                "nitrite",
                "nitrite",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("nitrite_meth", "nitrite_meth"),
            fld(
                "npic",
                "npic",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("npic_meth", "npic_meth"),
            fld(
                "npoc",
                "npoc",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("npoc_meth", "npoc_meth"),
            fld("nrs_sample_code", "nrs_sample_code"),
            fld("nrs_trip_code", "nrs_trip_code"),
            fld(
                "org_matter",
                "org_matter",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("org_matter_meth", "org_matter_meth"),
            fld(
                "organic_carbon",
                "organic_carbon",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("organic_carbon_meth", "organic_carbon_meth"),
            fld(
                "organic_fraction",
                "organic_fraction",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("organic_fraction_meth", "organic_fraction_meth"),
            fld(
                "osmium",
                "osmium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("osmium_meth", "osmium_meth"),
            fld(
                "oxygen",
                "oxygen",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("oxygen_meth", "oxygen_meth"),
            fld(
                "oxygen_ctd_vol",
                "oxygen_ctd_vol",
                units="mL/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("oxygen_ctd_vol_meth", "oxygen_ctd_vol_meth"),
            fld(
                "oxygen_ctd_wt",
                "oxygen_ctd_wt",
                units="\u03BC" + "mol/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("oxygen_ctd_wt_meth", "oxygen_ctd_wt_meth"),
            fld(
                "palladium",
                "palladium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("palladium_meth", "palladium_meth"),
            fld(
                "pam_fluorometer",
                "pam_fluorometer",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pam_fluorometer_meth", "pam_fluorometer_meth"),
            fld(
                "par",
                "par",
                units="\u03BC" + "mol/m2/s",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("par_meth", "par_meth"),
            fld(
                "part_org_carb",
                "part_org_carb",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("part_org_carb_meth", "part_org_carb_meth"),
            fld(
                "perid",
                "perid",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("perid_meth", "perid_meth"),
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
                "phide_a",
                "phide_a",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phide_a_meth", "phide_a_meth"),
            fld(
                "phosphate",
                "phosphate",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phosphate_meth", "phosphate_meth"),
            fld(
                "phosphorus_colwell",
                "phosphorus_colwell",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phosphorus_colwell_meth", "phosphorus_colwell_meth"),
            fld(
                "phytin_a",
                "phytin_a",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phytin_a_meth", "phytin_a_meth"),
            fld(
                "phytin_b",
                "phytin_b",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("phytin_b_meth", "phytin_b_meth"),
            fld(
                "picoeukaryotes",
                "picoeukaryotes",
                units="cells/mL",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("picoeukaryotes_meth", "picoeukaryotes_meth"),
            fld("plant_id", "plant_id"),
            fld("plant_stage", "plant_stage"),
            fld("plant_stage_meth", "plant_stage_meth"),
            fld(
                "platinum",
                "platinum",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("platinum_meth", "platinum_meth"),
            fld(
                "pn",
                "pn",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pn_meth", "pn_meth"),
            fld(
                "potassium",
                "potassium",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("potassium_meth", "potassium_meth"),
            fld(
                "potassium_colwell",
                "potassium_colwell",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("potassium_colwell_meth", "potassium_colwell_meth"),
            fld(
                "pras",
                "pras",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pras_meth", "pras_meth"),
            fld(
                "praseodymium",
                "praseodymium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("praseodymium_meth", "praseodymium_meth"),
            fld(
                "pres_rel",
                "pres_rel",
                units="dbar",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pres_rel_meth", "pres_rel_meth"),
            fld(
                "prochlorococcus",
                "prochlorococcus",
                units="cells/mL",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("prochlorococcus_meth", "prochlorococcus_meth"),
            fld("profile_position", "profile_position"),
            fld(
                "pyrophide_a",
                "pyrophide_a",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pyrophide_a_meth", "pyrophide_a_meth"),
            fld(
                "pyrophytin_a",
                "pyrophytin_a",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("pyrophytin_a_meth", "pyrophytin_a_meth"),
            fld(
                "rhodium", "rhodium", units="ppm", coerce=ingest_utils.get_clean_number
            ),
            fld("rhodium_meth", "rhodium_meth"),
            fld(
                "root_length",
                "root_length",
                units="cm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("root_length_meth", "root_length_meth"),
            fld("rosette_position", "rosette_position"),
            fld(
                "rubidium",
                "rubidium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("rubidium_meth", "rubidium_meth"),
            fld(
                "ruthenium",
                "ruthenium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("ruthenium_meth", "ruthenium_meth"),
            fld(
                "salinity",
                "salinity",
                units="PSU",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("salinity_meth", "salinity_meth"),
            fld(
                "salinity_lab",
                "salinity_lab",
                units="PSU",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("salinity_lab_meth", "salinity_lab_meth"),
            fld(
                "samarium",
                "samarium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("samarium_meth", "samarium_meth"),
            fld(
                "samp_size",
                "samp_size",
                units="L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "samp_vol_we_dna_ext",
                "samp_vol_we_dna_ext",
                units="L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sample_volume_notes", "sample_volume_notes"),
            fld("sand", "sand", units="%", coerce=ingest_utils.get_percentage),
            fld("sand_meth", "sand_meth"),
            fld(
                "scandium",
                "scandium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("scandium_meth", "scandium_meth"),
            fld(
                "secchi_depth",
                "secchi_depth",
                units="m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("secchi_depth_meth", "secchi_depth_meth"),
            fld(
                "sediment_grain_size",
                "sediment_grain_size",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("sediment_grain_size_meth", "sediment_grain_size_meth"),
            fld(
                "sediment_grain_size_fract",
                "sediment_grain_size_fract",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("sediment_grain_size_fract_meth", "sediment_grain_size_fract_meth"),
            fld(
                "sedimentation_rate",
                "sedimentation_rate",
                units="g/cm2/yr",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sedimentation_rate_meth", "sedimentation_rate_meth"),
            fld(
                "sediment_porewater_h4sio4",
                "sediment_porewater_h4sio4",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_h4sio4_meth", "sediment_porewater_h4sio4_meth"),
            fld(
                "sediment_porewater_nh4",
                "sediment_porewater_nh4",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_nh4_meth", "sediment_porewater_nh4_meth"),
            fld(
                "sediment_porewater_no2",
                "sediment_porewater_no2",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_no2_meth", "sediment_porewater_no2_meth"),
            fld(
                "sediment_porewater_no3",
                "sediment_porewater_no3",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_no3_meth", "sediment_porewater_no3_meth"),
            fld(
                "sediment_porewater_po43",
                "sediment_porewater_po43",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sediment_porewater_po43_meth", "sediment_porewater_po43_meth"),
            fld(
                "selenium",
                "selenium",
                units="\u03BC" + "g/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("selenium_meth", "selenium_meth"),
            fld(
                "shoot_length",
                "shoot_length",
                units="cm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("shoot_length_meth", "shoot_length_meth"),
            fld(
                "silicate",
                "silicate",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("silicate_meth", "silicate_meth"),
            fld("silt", "silt", units="%", coerce=ingest_utils.get_percentage),
            fld("silt_meth", "silt_meth"),
            fld(
                "silver",
                "silver",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("silver_meth", "silver_meth"),
            fld(
                "sio2",
                "sio2",
                units="\u03BC" + "mol/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sio2_meth", "sio2_meth"),
            fld("slope_aspect", "slope_aspect", units="direction_or_degrees"),
            fld("slope_aspect_meth", "slope_aspect_meth"),
            fld(
                "slope_gradient",
                "slope_gradient",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("slope_gradient_meth", "slope_gradient_meth"),
            fld(
                "sodium",
                "sodium",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sodium_meth", "sodium_meth"),
            fld("specific_host", "specific_host"),
            fld(
                "strontium",
                "strontium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("strontium_meth", "strontium_meth"),
            fld(
                "sulphur",
                "sulphur",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("sulphur_meth", "sulphur_meth"),
            fld(
                "synechococcus",
                "synechococcus",
                units="cells/mL",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("synechococcus_meth", "synechococcus_meth"),
            fld("synonyms", "synonyms"),
            fld(
                "tantalum",
                "tantalum",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("tantalum_meth", "tantalum_meth"),
            fld(
                "temp",
                "temp",
                units="degC",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("temp_meth", "temp_meth"),
            fld(
                "terbium",
                "terbium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("terbium_meth", "terbium_meth"),
            fld("texture", "texture"),
            fld("texture_meth", "texture_meth"),
            fld(
                "thorium", "thorium", units="ppm", coerce=ingest_utils.get_clean_number
            ),
            fld("thorium_meth", "thorium_meth"),
            fld(
                "thulium", "thulium", units="ppm", coerce=ingest_utils.get_clean_number
            ),
            fld("thulium_meth", "thulium_meth"),
            fld("tillage", "tillage"),
            fld("tin", "tin", units="ppm", coerce=ingest_utils.get_clean_number),
            fld("tin_meth", "tin_meth"),
            fld(
                "tot_carb",
                "tot_carb",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("tot_carb_meth", "tot_carb_meth"),
            fld(
                "tot_depth_water_col",
                "tot_depth_water_col",
                units="m",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("tot_depth_water_col_meth", "tot_depth_water_col_meth"),
            fld(
                "tot_nitro",
                "tot_nitro",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("tot_nitro_meth", "tot_nitro_meth"),
            fld(
                "tot_org_carb",
                "tot_org_carb",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("tot_org_c_meth", "tot_org_c_meth"),
            fld(
                "tot_phosp",
                "tot_phosp",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("tot_phosp_meth", "tot_phosp_meth"),
            fld(
                "total_co2",
                "total_co2",
                units="\u03BC" + "mol/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("total_co2_meth", "total_co2_meth"),
            fld(
                "total_inorganic_carbon",
                "total_inorganic_carbon",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("total_inorganic_carbon_meth", "total_inorganic_carbon_meth"),
            fld(
                "total_nitrogen",
                "total_nitrogen",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("total_nitrogen_meth", "total_nitrogen_meth"),
            fld(
                "total_phosphorous",
                "total_phosphorous",
                units="mg/kg",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("total_phosphorous_meth", "total_phosphorous_meth"),
            fld("touching_organisms", "touching_organisms"),
            fld(
                "transmittance",
                "transmittance",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("transmittance_meth", "transmittance_meth"),
            fld(
                "tss",
                "tss",
                units="mg/L",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("tss_meth", "tss_meth"),
            fld(
                "tungsten",
                "tungsten",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("tungsten_meth", "tungsten_meth"),
            fld(
                "turbidity",
                "turbidity",
                units="NTU",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("turbidity_meth", "turbidity_meth"),
            fld(
                "uranium", "uranium", units="ppm", coerce=ingest_utils.get_clean_number
            ),
            fld("uranium_meth", "uranium_meth"),
            fld("url", "url"),
            fld(
                "vanadium",
                "vanadium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("vanadium_meth", "vanadium_meth"),
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
            fld("vegetation_total_cover_meth", "vegetation_total_cover_meth"),
            fld(
                "viola",
                "viola",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("viola_meth", "viola_meth"),
            fld("voyage_code", "voyage_code"),
            fld("voyage_survey_link", "voyage_survey_link"),
            fld(
                "water_content",
                "water_content",
                units="%",
                coerce=ingest_utils.get_percentage,
            ),
            fld("water_content_soil_meth", "water_content_soil_meth"),
            fld(
                "water_holding_capacity",
                "water_holding_capacity",
                units="g/cm3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("water_holding_capacity_meth", "water_holding_capacity_meth"),
            fld(
                "ytterbium",
                "ytterbium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("ytterbium_meth", "ytterbium_meth"),
            fld(
                "yttrium", "yttrium", units="ppm", coerce=ingest_utils.get_clean_number
            ),
            fld("yttrium_meth", "yttrium_meth"),
            fld(
                "zea",
                "zea",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("zea_meth", "zea_meth"),
            fld(
                "zirconium",
                "zirconium",
                units="ppm",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("zirconium_meth", "zirconium_meth"),
            fld(
                "zooplankton_biomass",
                "zooplankton_biomass",
                units="mg/m3",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("zooplankton_biomass_meth", "zooplankton_biomass_meth"),
            fld(
                "sample_metadata_ingest_date",
                "sample_metadata_ingest_date",
            ),
            fld("sample_metadata_ingest_file", "sample_metadata_ingest_file"),
            fld("sample_metadata_update_history", "sample_metadata_update_history"),
            fld("sample_database_file", "sample_database_file"),
            fld("database_schema_definitions_url", "database_schema_definitions_url"),
        ]
    }

    def __init__(self, logger, path):
        self._logger = logger
        self.path_dir = path
        source_path = one(glob(path + self.source_pattern))
        self.initialise_source_path(source_path)

    def initialise_source_path(self, source_path):
        self.sample_metadata = self._package_metadata(self._read_metadata(source_path))

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


class MarineMicrobesNCBIContextual(NCBISRAContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/metadata/ncbi/"
    ]
    name = "mm-ncbi-contextual"
    bioproject_accession = "PRJNA385736"


class BASENCBIContextual(NCBISRAContextual):
    metadata_urls = ["https://downloads-qcif.bioplatforms.com/bpa/base/metadata/ncbi/"]
    name = "base-ncbi-contextual"
    bioproject_accession = "PRJNA317932"


class AustralianMicrobiomeDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/amd/dataset_control/2022-12-05/"
    ]
    name = "amd-dataset-contextual"
    contextual_linkage = ("sample_id",)
    sheet_names = [
        "Sheet1",
    ]
    related_data_identifier_type = "dataset_id"
    additional_fields = [
        fld("dataset_id", "bioplatforms_dataset_id"),
        fld("bioplatforms_dataset_id", "bioplatforms_dataset_id"),
        fld("bioplatforms_project_code", "bioplatforms_project_code"),
        fld("bioplatforms_project", "bioplatforms_project"),
        fld("ncbi_bioproject_accession_number", "ncbi_bioproject_accession_number"),
        fld("ncbi_biosample_accession_number", "ncbi_biosample_accession_number"),
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

    def sample_ids(self):
        if len(self.contextual_linkage) != 1:
            raise Exception("Linkage of unexpected length")

        # return a list of the first item of the linkage
        # This will be a BPA Sample ID
        return list(k[0] for k in self.dataset_metadata.keys())
