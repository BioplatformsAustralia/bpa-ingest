import datetime
import re
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper
from...util import make_logger

logger = make_logger(__name__)

CHEM_MIN_SENTINAL_VALUE = 0.0001


def get_float_or_sentinal(val):
    # if its a float, its probably ok
    if isinstance(val, float):
        return val

    # keep no data no data
    if val == '':
        return None

    # substitute the sentinal value for below threshold values
    if isinstance(val, basestring) and (val.find('<') != -1):
        return CHEM_MIN_SENTINAL_VALUE


def fix_sometimes_date(val):
    "mix of dates and free-text, make into strings"
    if type(val) is datetime.date or type(val) is datetime.datetime:
        return ingest_utils.get_date_isoformat(val)
    val = val.strip()
    if val == '':
        return None
    return val


def fix_slope_date(val):
    # 2/3 has been turned into a date by Excel
    if isinstance(val, datetime.datetime):
        return '%s/%s' % (val.day, val.month)
    return val


class BASESampleContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/base/metadata/']
    name = 'base-contextual'

    def __init__(self, path):
        xlsx_path = path + '/contextual-latest.xlsx'
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(bpa_id)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            assert(row.bpa_id)
            assert(row.bpa_id not in sample_metadata)
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'bpa_id':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('bpa_id', 'Sample_ID', ingest_utils.extract_bpa_id),
            ('date_sampled', 'Date sampled', ingest_utils.get_date_isoformat),
            ('latitude', 'latitude', ingest_utils.get_clean_number),
            ('longitude', 'longitude', ingest_utils.get_clean_number),
            ('depth', 'Depth', None),
            ('horizon_classification', 'Horizon', None),
            ('storage_method', 'soil sample storage method', None),
            ('description', 'location description', None),
            ('broad_land_use', 'broad land use', None),
            ('current_land_use', 'Detailed land use', None),
            ('general_ecological_zone', 'General Ecological Zone', None),
            ('vegetation_type_controlled_vocab', 'Vegetation Type', None),
            ('vegetation_total_cover', 'Vegetation Total cover (%)', None),
            ('vegetation_dominant_trees', 'Vegetation Dom. Trees (%)', None),
            ('elevation', 'Elevation ()', ingest_utils.get_clean_number),
            ('slope', 'Slope (%)', fix_slope_date),
            ('slope_aspect', re.compile(r'^slope aspect .*'), None),
            ('profile_position', 'Profile Position controlled vocab (5)', None),
            ('australian_soil_classification', 'Australian Soil Classification controlled vocab (6)', None),
            ('fao', 'FAO soil classification controlled vocab (7)', None),
            # historic data
            ('immediate_previous_land_use', 'Immediate Previous Land Use controlled vocab (2)', None),
            ('date_since_change_in_land_use', 'Date since change in Land Use', None),
            ('crop_rotation_1', 'Crop rotation 1yr since present', None),
            ('crop_rotation_2', 'Crop rotation 2yrs since present', None),
            ('crop_rotation_3', 'Crop rotation 3yrs since present', None),
            ('crop_rotation_4', 'Crop rotation 4yrs since present', None),
            ('crop_rotation_5', 'Crop rotation 5yrs since present', None),
            ('agrochemical_additions', 'Agrochemical Additions', None),
            ('tillage', 'Tillage controlled vocab (9)', None),
            ('fire_history', 'Fire', fix_sometimes_date),
            ('fire_intensity', 'fire intensity if known', None),
            ('flooding', 'Flooding', fix_sometimes_date),
            ('extreme_event', 'Extreme Events', None),
            # soil structual
            ('soil_moisture', 'Soil moisture (%)', ingest_utils.get_clean_number),
            ('soil_colour', 'Color controlled vocab (10)', None),
            ('gravel', 'Gravel (%) - ( >2.0 mm)', None),
            ('texture', 'Texture ()', ingest_utils.get_clean_number),
            ('course_sand', re.compile(r'^course sand .*'), ingest_utils.get_clean_number),
            ('fine_sand', re.compile(r'^fine sand .*'), ingest_utils.get_clean_number),
            ('sand', 'Sand (%)', ingest_utils.get_clean_number),
            ('silt', re.compile(r'^silt .*'), ingest_utils.get_clean_number),
            ('clay', re.compile(r'^clay .*'), ingest_utils.get_clean_number),
            # soil chemical
            ('ammonium_nitrogen', 'Ammonium Nitrogen (mg/Kg)', get_float_or_sentinal),
            ('nitrate_nitrogen', 'Nitrate Nitrogen (mg/Kg)', get_float_or_sentinal),
            ('phosphorus_collwell', 'Phosphorus Colwell (mg/Kg)', get_float_or_sentinal),
            ('potassium_collwell', 'Potassium Colwell (mg/Kg)', get_float_or_sentinal),
            ('sulphur', 'Sulphur (mg/Kg)', get_float_or_sentinal),
            ('organic_carbon', 'Organic Carbon (%)', get_float_or_sentinal),
            ('conductivity', 'Conductivity (dS/m)', get_float_or_sentinal),
            ('cacl2_ph', 'pH Level (CaCl2) (pH)', get_float_or_sentinal),
            ('h20_ph', 'pH Level (H2O) (pH)', get_float_or_sentinal),
            ('dtpa_copper', 'DTPA Copper (mg/Kg)', get_float_or_sentinal),
            ('dtpa_iron', 'DTPA Iron (mg/Kg)', get_float_or_sentinal),
            ('dtpa_manganese', 'DTPA Manganese (mg/Kg)', get_float_or_sentinal),
            ('dtpa_zinc', 'DTPA Zinc (mg/Kg)', get_float_or_sentinal),
            ('exc_aluminium', 'Exc. Aluminium (meq/100g)', get_float_or_sentinal),
            ('exc_calcium', 'Exc. Calcium (meq/100g)', get_float_or_sentinal),
            ('exc_magnesium', 'Exc. Magnesium (meq/100g)', get_float_or_sentinal),
            ('exc_potassium', 'Exc. Potassium (meq/100g)', get_float_or_sentinal),
            ('exc_sodium', 'Exc. Sodium (meq/100g)', get_float_or_sentinal),
            ('boron_hot_cacl2', 'Boron Hot CaCl2 (mg/Kg)', get_float_or_sentinal),
            ('total_nitrogen', 'Total Nitrogen', get_float_or_sentinal),
            ('total_carbon', 'Total Carbon', get_float_or_sentinal),
            ('methodological_notes', 'Methodological notes', None),
            ('other_comments', 'Other comments', None), ]

        wrapper = ExcelWrapper(field_spec, metadata_path, sheet_name='Sample_info', header_length=1, column_name_row_index=0)

        return wrapper.get_all()
