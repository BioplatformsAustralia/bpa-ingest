# -*- coding: utf-8 -*-
# http://codeinthehole.com/writing/prefer-data-migrations-to-initial-data/

# 1) Horizon

HorizonClassificationVocabulary = (
    (
        "O",
        "The organic or litter layer mainly over the soil surface composed of fresh and decaying plant residue. Typically composed of >25% organic soil materials.  This layer can usually be easily brushed away by hand.",
    ),
    (
        "A",
        "This is the top layer of soil (often called top soil) that typically has the highest microbial activity.  It is composed of a mixture of mineral material and accumulated humified organic matter. Also, any plowed or disturbed surface layer.",
    ),
    (
        "E",
        "This is the eluviated layer that is only seen in older soils and is not a universally used horizon designation.  If present it is below the A horizon and commonly is recognized by its lighter colored due to loss of clay, iron, aluminum, organic matter or some combination of these components.",
    ),
    (
        "B",
        "In lay terms this is often called sub-soil.  It is the layer where most of the mineral materials that leach from the upper horizons accumulate.  Accumulation of clay, iron, silica, calcium carbonate, calcium sulphate, sesquioxides, and/or humus can occur.  Soil structure and color often differs from the A horizon. Color may be redder or browner due to iron accumulation.  Structure may be more granular, prismatic, or blocky.",
    ),
    (
        "C",
        "This is basically the weathered parent rock (excluding hard bedrock) that has not been affected by soil formation.  Composed mainly of large rocks.",
    ),
    (
        "R",
        "This is the consolidated unweathered hard bedrock beneath the soil.  Usually is one large continuous rock mass.  The bedrock commonly found below the C horizon but under certain circumstances can be found directly below the A or B horizon.",
    ),
    (
        "P",
        "Soils that are permanently frozen due to sustained temperature of less than 0ºC for two or more years.  Typically found in Polar regions.",
    ),
)

# 2) land use
LandUseVocabulary = (
    (
        "Conservation and Natural Environments",
        (
            "Nature conservation",
            (
                "Strict nature reserves",
                "Wilderness area",
                "National park",
                "Natural feature protection",
                "Habitat/species management area",
                "Protected landscape",
                "Other conserved area",
            ),
        ),
        (
            "Managed resource protection",
            (
                "Biodiversity",
                "Surface water supply",
                "Groundwater",
                "Landscape",
                "Traditional indigenous uses",
            ),
        ),
        (
            "Other minimal use",
            (
                "Defence land - natural areas",
                "Stock route",
                "Residual native cover",
                "Rehabilitation",
            ),
        ),
    ),
    (
        "Production from Relatively Natural Environments",
        (
            "Grazing native vegetation",
            (),
        ),
        (
            "Production forestry",
            ("Wood production", "Other forest production"),
        ),
    ),
    (
        "Production from Dryland Agriculture and Plantations",
        (
            "Plantation forestry",
            (
                "Hardwood plantation",
                "Softwood plantation",
                "Other forest plantation",
                "Environmental forest plantation",
            ),
        ),
        (
            "Grazing modified pastures",
            (
                "Native/exotic pasture mosaic",
                "Woody fodder plants",
                "Pasture legumes",
                "Pasture legume/grass mixtures",
                "Sown grasses",
            ),
        ),
        (
            "Cropping",
            (
                "Cereals -wheat",
                "Cereals -barley",
                "Cereals -maize",
                "Cereals -triticale",
                "Cereals -oats",
                "Cereals -sorghum",
                "Cereals -other",
                "Beverage and spice crops -coffee",
                "Beverage and spice crops -tea",
                "Beverage and spice crops -cocoa",
                "Beverage and spice crops -other",
                "Hay and silage -grasses",
                "Hay and silage -grasses and legume mix",
                "Hay and silage -pure legume",
                "Hay and silage -other",
                "Oil seeds -canola",
                "Oil seeds -sunflower",
                "Oil seeds -flax",
                "Oil seeds -soybean",
                "Oil seeds -other",
                "Sugar",
                "Cotton",
                "Alkaloid poppies",
                "Pulses -lupin",
                "Pulses -lentil",
                "Pulses -chickpea",
                "Pulses -vetch",
                "Pulses -faba/broad bean",
                "Pulses -azuki bean",
                "Pulses -mung bean",
                "Pulses -other",
            ),
        ),
        (
            "Perennial horticulture",
            (
                "Tree fruits -apple",
                "Tree fruits -pear",
                "Tree fruits -avocado",
                "Tree fruits -banana",
                "Tree fruits -cherry",
                "Tree fruits -mango",
                "Tree fruits -pineapple",
                "Tree fruits -other",
                "Oleaginous fruits -olive",
                "Oleaginous fruits -other",
                "Tree nuts -macadamia",
                "Tree nuts -almond",
                "Tree nuts -other",
                "Vine fruits",
                "Shrub nuts, fruits and berrie -strawberry",
                "Shrub nuts, fruits and berrie -berry fruits",
                "Shrub nuts, fruits and berrie -other",
                "Perennial flowers and bulbs",
                "Perennial vegetables and herbs",
                "Citrus",
                "Grapes -wine grape",
                "Grapes -table grape",
            ),
        ),
        (
            "Seasonal horticulture",
            (
                "Seasonal fruits",
                "Seasonal nuts",
                "Seasonal flowers and bulbs",
                "Seasonal vegetables and herbs",
            ),
        ),
        (
            "Land in transition",
            (
                "Degraded land",
                "Abandoned land",
                "Land under rehabilitation",
                "No defined use",
                "Abandoned perennial horticulture",
            ),
        ),
    ),
    (
        "Production from Irrigated Agriculture and Plantations",
        (
            "Irrigated plantation forestry",
            (
                "Irrigated hardwood plantation",
                "Irrigated softwood plantation",
                "Irrigated other forest plantation",
                "Irrigated environmental forest plantation",
            ),
        ),
        (
            "Grazing irrigated modified pastures",
            (
                "Irrigated woody fodder plants",
                "Irrigated pasture legumes",
                "Irrigated legume/grass mixtures",
                "Irrigated sown grasses",
            ),
        ),
        (
            "Irrigated cropping",
            (
                "Irrigated cereals",
                "Irrigated beverage and spice crops",
                "Irrigated hay and silage",
                "Irrigated oil seeds",
                "Irrigated sugar",
                "Irrigated cotton",
                "Irrigated alkaloid poppies",
                "Irrigated pulses",
                "Irrigated rice",
            ),
        ),
        (
            "Irrigated perennial horticulture",
            (
                "Irrigated tree fruits",
                "Irrigated oleaginous fruits",
                "Irrigated tree nuts",
                "Irrigated vine fruits",
                "Irrigated shrub nuts, fruits and berries",
                "Irrigated perennial flowers and bulbs",
                "Irrigated perennial vegetables and herbs",
                "Irrigated citrus",
                "Irrigated grapes",
            ),
        ),
        (
            "Irrigated seasonal horticulture",
            (
                "Irrigated seasonal fruits",
                "Irrigated seasonal nuts",
                "Irrigated seasonal flowers and bulbs",
                "Irrigated seasonal vegetables and herbs",
                "Irrigated turf farming",
            ),
        ),
        (
            "Irrigated land in transition",
            (
                "Degraded irrigated land",
                "Abandoned irrigated land",
                "Irrigated land under rehabilitation",
                "No defined use (irrigation)",
                "Abandoned irrigated perennial horticulture",
            ),
        ),
    ),
    (
        "Intensive Uses",
        (
            "Intensive horticulture",
            (
                "Shadehouses",
                "Glasshouses",
                "Glasshouses (hydroponic)",
                "Abandoned intensive horticulture",
            ),
        ),
        (
            "Intensive animal husbandry",
            (
                "Dairy sheds and yards",
                "Cattle feedlots",
                "Sheep feedlots",
                "Poultry farms",
                "Piggeries",
                "Aquaculture",
                "Horse studs",
                "Stockyards/saleyards",
                "Abandoned intensive animal husbandry",
            ),
        ),
        (
            "Manufacturing and industrial",
            (
                "General purpose factory",
                "Food processing factory",
                "Major industrial complex",
                "Bulk grain storage",
                "Abattoirs",
                "Oil refinery",
                "Sawmill",
                "Abandoned manufacturing and industrial",
            ),
        ),
        (
            "Residential and farm infrastructure",
            (
                "Urban residential",
                "Rural residential with agriculture",
                "Rural residential without agriculture",
                "Remote communities",
                "Farm buildings/infrastructure",
            ),
        ),
        (
            "Services",
            (
                "Commercial services",
                "Public services",
                "Recreation and culture",
                "Defence facilities - urban",
                "Research facilities",
            ),
        ),
        (
            "Utilities",
            (
                "Fuel powered electricity generation",
                "Hydro electricity generation",
                "Wind farm electricity generation",
                "Electricity substations and transmission",
                "Gas treatment, storage and transmission",
                "Water extraction and transmission",
            ),
        ),
        (
            "Transport and communication",
            (
                "Airports/aerodromes",
                "Roads",
                "Railways",
                "Ports and water transport",
                "Navigation and communication",
            ),
        ),
        (
            "Mining",
            ("Mines", "Quarries", "Tailings", "Extractive industry not in use"),
        ),
        (
            "Waste treatment and disposal",
            (
                "Landfill",
                "Effluent pond",
                "Solid garbage",
                "Incinerators",
                "Sewage/sewerage",
            ),
        ),
    ),
    (
        "Water",
        (
            "Lake",
            (
                "Lake - conservation",
                "Lake - production",
                "Lake - intensive use",
                "Lake - saline",
            ),
        ),
        (
            "Reservoir/dam",
            (
                "Reservoir",
                "Water storage - intensive use/farm dams",
                "Evaporation basin",
            ),
        ),
        (
            "River",
            ("River - conservation", "River - production", "River - intensive use"),
        ),
        (
            "Channel/aqueduct",
            ("Supply channel/aqueduct", "Drainage channel/aqueduct", "Stormwater"),
        ),
        (
            "Marsh/wetland",
            (
                "Marsh/wetland - conservation",
                "Marsh/wetland - production",
                "Marsh/wetland - intensive use",
                "Marsh/wetland - saline",
            ),
        ),
        (
            "Estuary/coastal waters",
            (
                "Estuary/coastal waters - conservation",
                "Estuary/coastal waters - production",
                "Estuary/coastal waters - intensive use",
            ),
        ),
    ),
)

# 3) Ecological Zone
EcologicalZoneVocabulary = (
    ("Arid", "Hot and Dry"),
    ("Temperate", ""),
    ("Tropical (dry)", "Low rainfall"),
    ("Tropical (wet)", "High rainfall"),
    ("Coastal", ""),
    ("Montane", ""),
    ("Riverine", ""),
    ("Estuarine", ""),
    ("Mediterranean", ""),
    ("Polar", ""),
    ("Other", ""),
)

# 4) Vegetation Type
BroadVegetationTypeVocabulary = (
    ("Marsh/bog", ""),
    ("Heathland", ""),
    ("Grassland", ""),
    ("Shrubland", ""),
    ("Woodland", ""),
    ("Forest", ""),
    ("Savannah", ""),
    ("Dune", ""),
    ("Other", ""),
)

# 5) Profile position
ProfilePositionVocabulary = (
    ("Summit/ridge", ""),
    ("Upper slope", ""),
    ("Mid slope", ""),
    ("Lower slope", ""),
    ("Valley floor", ""),
    ("Depression", ""),
)

# 6) Australian Soil Classification
AustralianSoilClassificationVocabulary = (
    ("Anthroposols", ""),
    ("Organosols", ""),
    ("Podosols", ""),
    ("Vertosols", ""),
    ("Hydrosols", ""),
    ("Kurosols", ""),
    ("Sodosols", ""),
    ("Chromosols", ""),
    ("Calcarosols", ""),
    ("Ferrosols", ""),
    ("Dermosols", ""),
    ("Kandosols", ""),
    ("Rudosols", ""),
    ("Tenosols", ""),
)

# 7) FAO Soil Classification
FAOSoilClassificationVocabulary = (
    ("HISTOSOLS", ""),
    ("CRYOSOLS", ""),
    ("NTHROSOLS", ""),
    ("LEPTOSOLS", ""),
    ("VERTISOLS", ""),
    ("FLUVISOLS", ""),
    ("SOLONCHAKS", ""),
    ("GLEYSOLS", ""),
    ("ANDOSOLS", ""),
    ("PODZOLS", ""),
    ("PLINTHOSOLS", ""),
    ("FERRALSOLS", ""),
    ("SOLONETZ", ""),
    ("PLANOSOLS", ""),
    ("CHERNOZEMS", ""),
    ("KASTANOZEMS", ""),
    ("PHAEOZEMS", ""),
    ("GYPSISOLS", ""),
    ("DURISOLS", ""),
    ("CALCISOLS", ""),
    ("ALBELUVISOLS", ""),
    ("ALISOLS", ""),
    ("NITISOLS", ""),
    ("ACRISOLS", ""),
    ("LUVISOLS", ""),
    ("LIXISOLS", ""),
    ("UMBRISOLS", ""),
    ("CAMBISOLS", ""),
    ("ARENOSOLS", ""),
    ("REGOSOLS", ""),
)

# 8) Crop Rotation
CropRotationClassification = (
    "No crop",
    "Cereals - wheat",
    "Cereals - barley",
    "Cereals - maize",
    "Cereals - triticale",
    "Cereals - oats",
    "Cereals - sorghum",
    "Cereals - other",
    "Beverage and spice crops - coffee",
    "Beverage and spice crops - tea",
    "Beverage and spice crops - cocoa",
    "Beverage and spice crops - other",
    "Hay and silage - grasses",
    "Hay and silage - grasses and legume mix",
    "Hay and silage - pure legume",
    "Hay and silage - other",
    "Oil seeds - canola",
    "Oil seeds - sunflower",
    "Oil seeds - flax",
    "Oil seeds -soybean",
    "Oil seeds -other",
    "Sugar",
    "Cotton",
    "Alkaloid poppies",
    "Pulses - lupin",
    "Pulses - lentil",
    "Pulses - chickpea",
    "Pulses - vetch",
    "Pulses - faba/broad bean",
    "Pulses - azuki bean",
    "Pulses - mung bean",
    "Pulses - other",
    "Seasonal vegetables and herbs - potato",
    "Seasonal vegetables and herbs - other",
)

# 9) Tillage
TillageClassificationVocabulary = (
    (
        "Conservation Tillage or Zero Tillage",
        "Tillage system that does not invert plant material into the soil, crop residues are left on the surface and crops are planted through the residue using a drill or cutting disc.",
    ),
    (
        "Reduced Tillage",
        "Less intensity of cultivation than conventional tillage.  Minimal soil disturbance typically limited to planting area.  Common examples are ridge till, strip tillage, zonal tillage, chisel or tined.",
    ),
    (
        "Conventional Tillage",
        "Soil is mechanically inverted typically using a mouldboard or a disc plough. Crop residues and weeds are incorporated into the soil and larger aggregates are broken down to prepare a seedbed.",
    ),
)

# 10 Colour
SoilColourVocabulary = (
    ("Brown", "BR"),
    ("Gray", "GR"),
    ("Black", "BK"),
    ("Brown Gray", "BRGR"),
    ("Gray Brown", "GRBR"),
    ("Light Brown", "LTBR"),
    ("Dark Brown", "DKBR"),
    ("Light Gray", "LTGR"),
    ("Dark Gray", "DKGR"),
    ("Brown Yellow", "BRYW"),
    ("Brown Red", "BRRD"),
    ("Brown Orange", "BROR"),
    ("Brown Black", "BRBK"),
    ("Brown White", "BRWH"),
    ("Gray Black", "GRBK"),
    ("Gray White", "GRWH"),
    ("Gray Yellow", "GRYW"),
    ("White", "WH"),
    ("Yellow", "YW"),
    ("Yellow Brown", "YWBR"),
    ("Yellow Gray", "YWGR"),
    ("Orange", "OR"),
)

DrainageClassificationVocabulary = (
    (
        "Excessively drained",
        "Water is removed from the soil very rapidly and available water holding capacity is very low.  Soils are commonly very sandy, gravelly or shallow on steep slopes All are uniform color and free of the mottling (mixture of colors in the same layer, often yellow, brown and sometimes grey) related to wetness. Irrigation would be needed for crop production.",
    ),
    (
        "Somewhat excessively drained",
        "Water is removed from the soil rapidly. Internal free water occurrence commonly is very rare or very deep. The soils are commonly coarse-textured and have high saturated hydraulic conductivity or are very shallow.",
    ),
    (
        "Well drained",
        "Water is removed from the soil readily, but not rapidly making it available to plants throughout most of the growing season.  Seasonal high water table is not within the rooting zone long enough during the growing season to adversely affect agricultural crops. Soils are commonly medium textured and mainly free of mottling.",
    ),
    (
        "Moderately well drained",
        "Water is removed from the soil somewhat slowly during some periods. Soils are wet for only a short time during the growing season, but periodically they are wet long enough that most mesophytic crops are affected. They commonly have a slowly pervious layer within or directly below the solum, or periodically receive high rainfall, or both.",
    ),
    (
        "Somewhat poorly drained",
        "Water is removed slowly enough that the soil is wet for significant periods during the growing season. Wetness markedly restricts the growth of mesophytic crops unless artificial drainage is provided. Soils commonly have a slowly pervious layer, a high water table, additional water from seepage, nearly continuous rainfall, or a combination of these factors.",
    ),
    (
        "Poorly drained",
        "Water is removed so slowly that the soil is saturated periodically during the growing season or remains wet for long periods. Free water is commonly at or near the surface for long enough during the growing season that most mesophytic crops cannot be grown unless the soil is artificially drained. The soil is not continuously saturated in layers directly below plow depth. Poor drainage results from a high water table, a slowly pervious layer within the profile, seepage, nearly continuous rainfall, or a combination of these.",
    ),
    (
        "Very poorly drained",
        "Water is removed from the soil so slowly that free water remains at or on the surface during most of the growing season. Unless the soil is artificially drained, most mesophytic crops cannot be grown. Very poorly drained soils are commonly level or depressed and are frequently ponded. Yet in rare cases, where rainfall is high and nearly continuous, they can have moderate or high slope gradients.",
    ),
)
