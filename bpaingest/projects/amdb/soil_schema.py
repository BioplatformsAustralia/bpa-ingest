# contextual-2017-06-28.xlsx: Sheet1
sheet1_fields = [SchemaDatasetField(**t) for t in [
    {
        "field_name": "sample_id",
        "label": "Sample_ID",
        "python_type": "str"
    },
    {
        "field_name": "date_sampled",
        "label": "Date sampled",
        "python_type": "str"
    },
    {
        "field_name": "latitude",
        "label": "latitude",
        "python_type": "str"
    },
    {
        "field_name": "longitude",
        "label": "longitude",
        "python_type": "str"
    },
    {
        "field_name": "depth",
        "label": "Depth",
        "python_type": "str"
    },
    {
        "field_name": "horizon",
        "label": "Horizon",
        "python_type": "str"
    },
    {
        "field_name": "soil_sample_storage_method",
        "label": "soil sample storage method",
        "python_type": "str"
    },
    {
        "field_name": "geo_loc",
        "label": "geo_loc",
        "python_type": "str"
    },
    {
        "field_name": "location_description",
        "label": "location description",
        "python_type": "str"
    },
    {
        "field_name": "broad_land_use",
        "label": "broad land use",
        "python_type": "str"
    },
    {
        "field_name": "detailed_land_use",
        "label": "Detailed land use",
        "python_type": "str"
    },
    {
        "field_name": "general_ecological_zone",
        "label": "General Ecological Zone",
        "python_type": "str"
    },
    {
        "field_name": "vegetation_type",
        "label": "Vegetation Type",
        "python_type": "str"
    },
    {
        "field_name": "vegetation_total_cover",
        "label": "Vegetation Total cover (%)",
        "python_type": "str"
    },
    {
        "field_name": "vegetation_dom_trees",
        "label": "Vegetation Dom. Trees (%)",
        "python_type": "str"
    },
    {
        "field_name": "vegetation_dom_shrubs",
        "label": "Vegetation Dom. Shrubs (%)",
        "python_type": "str"
    },
    {
        "field_name": "vegetation_dom_grasses",
        "label": "Vegetation Dom. Grasses (%)",
        "python_type": "str"
    },
    {
        "field_name": "elevation",
        "label": "Elevation ()",
        "python_type": "str"
    },
    {
        "field_name": "slope",
        "label": "Slope (%)",
        "python_type": "str"
    },
    {
        "field_name": "slope_aspect",
        "label": "Slope Aspect (Direction or degrees; e.g., NW or 315\u00b0)",
        "python_type": "str"
    },
    {
        "field_name": "profile_position_controlled_vocab",
        "label": "Profile Position controlled vocab (5)",
        "python_type": "str"
    },
    {
        "field_name": "australian_soil_classification_controlled_vocab",
        "label": "Australian Soil Classification controlled vocab (6)",
        "python_type": "str"
    },
    {
        "field_name": "fao_soil_classification_controlled_vocab",
        "label": "FAO soil classification controlled vocab (7)",
        "python_type": "str"
    },
    {
        "field_name": "immediate_previous_land_use_controlled_vocab",
        "label": "Immediate Previous Land Use controlled vocab (2)",
        "python_type": "str"
    },
    {
        "field_name": "date_since_change_in_land_use",
        "label": "Date since change in Land Use",
        "python_type": "str"
    },
    {
        "field_name": "crop_rotation_1yr_since_present",
        "label": "Crop rotation 1yr since present",
        "python_type": "str"
    },
    {
        "field_name": "crop_rotation_2yrs_since_present",
        "label": "Crop rotation 2yrs since present",
        "python_type": "str"
    },
    {
        "field_name": "crop_rotation_3yrs_since_present",
        "label": "Crop rotation 3yrs since present",
        "python_type": "str"
    },
    {
        "field_name": "crop_rotation_4yrs_since_present",
        "label": "Crop rotation 4yrs since present",
        "python_type": "str"
    },
    {
        "field_name": "crop_rotation_5yrs_since_present",
        "label": "Crop rotation 5yrs since present",
        "python_type": "str"
    },
    {
        "field_name": "agrochemical_additions",
        "label": "Agrochemical Additions",
        "python_type": "str"
    },
    {
        "field_name": "tillage_controlled_vocab",
        "label": "Tillage controlled vocab (9)",
        "python_type": "str"
    },
    {
        "field_name": "fire",
        "label": "Fire",
        "python_type": "str"
    },
    {
        "field_name": "fire_intensity_if_known",
        "label": "fire intensity if known",
        "python_type": "str"
    },
    {
        "field_name": "flooding",
        "label": "Flooding",
        "python_type": "str"
    },
    {
        "field_name": "extreme_events",
        "label": "Extreme Events",
        "python_type": "str"
    },
    {
        "field_name": "soil_moisture",
        "label": "Soil moisture (%)",
        "python_type": "str"
    },
    {
        "field_name": "color_controlled_vocab",
        "label": "Color controlled vocab (10)",
        "python_type": "str"
    },
    {
        "field_name": "gravel",
        "label": "Gravel (%)- ( >2.0 mm)",
        "python_type": "str"
    },
    {
        "field_name": "texture",
        "label": "Texture ()",
        "python_type": "str"
    },
    {
        "field_name": "course_sand",
        "label": "Course Sand (%) (200-2000 \u00b5m)",
        "python_type": "str"
    },
    {
        "field_name": "fine_sand",
        "label": "Fine Sand (%) - (20-200 \u00b5m)",
        "python_type": "str"
    },
    {
        "field_name": "sand",
        "label": "Sand (%)",
        "python_type": "str"
    },
    {
        "field_name": "silt",
        "label": "Silt  (%) (2-20 \u00b5m)",
        "python_type": "str"
    },
    {
        "field_name": "clay",
        "label": "Clay (%) (<2 \u00b5m)",
        "python_type": "str"
    },
    {
        "field_name": "ammonium_nitrogen",
        "label": "Ammonium Nitrogen (mg/Kg)",
        "python_type": "str"
    },
    {
        "field_name": "nitrate_nitrogen",
        "label": "Nitrate Nitrogen (mg/Kg)",
        "python_type": "str"
    },
    {
        "field_name": "phosphorus_colwell",
        "label": "Phosphorus Colwell (mg/Kg)",
        "python_type": "str"
    },
    {
        "field_name": "potassium_colwell",
        "label": "Potassium Colwell (mg/Kg)",
        "python_type": "str"
    },
    {
        "field_name": "sulphur",
        "label": "Sulphur (mg/Kg)",
        "python_type": "str"
    },
    {
        "field_name": "organic_carbon",
        "label": "Organic Carbon (%)",
        "python_type": "str"
    },
    {
        "field_name": "conductivity",
        "label": "Conductivity (dS/m)",
        "python_type": "str"
    },
    {
        "field_name": "ph_level",
        "label": "pH Level (CaCl2) (pH)",
        "python_type": "str"
    },
    {
        "field_name": "ph_level",
        "label": "pH Level (H2O) (pH)",
        "python_type": "str"
    },
    {
        "field_name": "dtpa_copper",
        "label": "DTPA Copper (mg/Kg)",
        "python_type": "str"
    },
    {
        "field_name": "dtpa_iron",
        "label": "DTPA Iron (mg/Kg)",
        "python_type": "str"
    },
    {
        "field_name": "dtpa_manganese",
        "label": "DTPA Manganese (mg/Kg)",
        "python_type": "str"
    },
    {
        "field_name": "dtpa_zinc",
        "label": "DTPA Zinc (mg/Kg)",
        "python_type": "str"
    },
    {
        "field_name": "exc_aluminium",
        "label": "Exc. Aluminium (meq/100g)",
        "python_type": "str"
    },
    {
        "field_name": "exc_calcium",
        "label": "Exc. Calcium (meq/100g)",
        "python_type": "str"
    },
    {
        "field_name": "exc_magnesium",
        "label": "Exc. Magnesium (meq/100g)",
        "python_type": "str"
    },
    {
        "field_name": "exc_potassium",
        "label": "Exc. Potassium (meq/100g)",
        "python_type": "str"
    },
    {
        "field_name": "exc_sodium",
        "label": "Exc. Sodium (meq/100g)",
        "python_type": "str"
    },
    {
        "field_name": "boron_hot_cacl2",
        "label": "Boron Hot CaCl2 (mg/Kg)",
        "python_type": "str"
    }
]]
