import datetime
from glob import glob
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper, make_field_definition as fld
from ...util import make_logger, one
from ...ncbi import NCBISRAContextual

logger = make_logger(__name__)

CHEM_MIN_SENTINAL_VALUE = 0.0001


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


class ContextualBase(object):
    def get(self, *args, **kwargs):
        return {}

    def filename_metadata(self, *args, **kwargs):
        return {}


class BASESampleContextual(ContextualBase):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/base/metadata/contextual/2017-06-28/']
    name = 'base-contextual'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(bpa_id)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.bpa_id is None:
                continue
            assert(row.bpa_id not in sample_metadata)
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                val = getattr(row, field)
                if field == 'latitude':
                    if val and val > 0:
                        logger.warning("Positioned in northern hemisphere, inverting: %s / %s" % (row.bpa_id, val))
                        val *= -1
                if field != 'bpa_id':
                    row_meta[field] = val
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld('bpa_id', 'sample_id', coerce=ingest_utils.extract_bpa_id),
            fld('date_sampled', 'date sampled', coerce=ingest_utils.get_date_isoformat),
            fld('latitude', 'latitude', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude', coerce=ingest_utils.get_clean_number),
            fld('depth', 'depth'),
            fld('horizon_classification', 'horizon'),
            fld('soil_sample_storage_method', 'soil sample storage method'),
            fld('geo_loc_name', 'geo_loc'),
            fld('location_description', 'location description'),
            fld('broad_land_use', 'broad land use'),
            fld('detailed_land_use', 'detailed land use'),
            fld('general_ecological_zone', 'general ecological zone'),
            fld('vegetation_type', 'vegetation type'),
            fld('vegetation_total_cover', 'vegetation total cover (%)'),
            fld('vegetation_dom_trees', 'vegetation dom. trees (%)'),
            fld('vegetation_dom_shrubs', 'vegetation dom. shrubs (%)'),
            fld('vegetation_dom_grasses', 'vegetation dom. grasses (%)'),
            fld('elevation', 'elevation ()', coerce=ingest_utils.get_clean_number),
            fld('slope', 'slope (%)', coerce=fix_slope_date),
            fld('slope_aspect', 'slope aspect (direction or degrees; e.g., nw or 315)'),
            fld('profile_position', 'profile position controlled vocab (5)'),
            fld('australian_soil_classification', 'australian soil classification controlled vocab (6)'),
            fld('fao_soil_classification', 'fao soil classification controlled vocab (7)'),
            fld('immediate_previous_land_use', 'immediate previous land use controlled vocab (2)'),
            fld('date_since_change_in_land_use', 'date since change in land use'),
            fld('crop_rotation_1yr_since_present', 'crop rotation 1yr since present'),
            fld('crop_rotation_2yrs_since_present', 'crop rotation 2yrs since present'),
            fld('crop_rotation_3yrs_since_present', 'crop rotation 3yrs since present'),
            fld('crop_rotation_4yrs_since_present', 'crop rotation 4yrs since present'),
            fld('crop_rotation_5yrs_since_present', 'crop rotation 5yrs since present'),
            fld('agrochemical_additions', 'agrochemical additions'),
            fld('tillage', 'tillage controlled vocab (9)'),
            fld('fire_history', 'fire', coerce=fix_sometimes_date),
            fld('fire_intensity_if_known', 'fire intensity if known'),
            fld('flooding', 'flooding', coerce=fix_sometimes_date),
            fld('extreme_events', 'extreme events'),
            fld('soil_moisture', 'soil moisture (%)', coerce=ingest_utils.get_clean_number),
            fld('color', 'color controlled vocab (10)'),
            fld('gravel', 'gravel (%)- ( >2.0 mm)'),
            fld('texture', 'texture ()', coerce=ingest_utils.get_clean_number),
            fld('course_sand', 'course sand (%) (200-2000 m)', coerce=ingest_utils.get_clean_number),
            fld('fine_sand', 'fine sand (%) - (20-200 m)', coerce=ingest_utils.get_clean_number),
            fld('sand', 'sand (%)', coerce=ingest_utils.get_clean_number),
            fld('silt', 'silt  (%) (2-20 m)', coerce=ingest_utils.get_clean_number),
            fld('clay', 'clay (%) (<2 m)', coerce=ingest_utils.get_clean_number),
            fld('ammonium_nitrogen', 'ammonium nitrogen (mg/kg)'),
            fld('nitrate_nitrogen', 'nitrate nitrogen (mg/kg)'),
            fld('phosphorus_colwell', 'phosphorus colwell (mg/kg)'),
            fld('potassium_colwell', 'potassium colwell (mg/kg)'),
            fld('sulphur', 'sulphur (mg/kg)'),
            fld('organic_carbon', 'organic carbon (%)'),
            fld('conductivity', 'conductivity (ds/m)'),
            fld('ph_level_cacl2', 'ph level (cacl2) (ph)'),
            fld('ph_level_h2o', 'ph level (h2o) (ph)'),
            fld('dtpa_copper', 'dtpa copper (mg/kg)'),
            fld('dtpa_iron', 'dtpa iron (mg/kg)'),
            fld('dtpa_manganese', 'dtpa manganese (mg/kg)'),
            fld('dtpa_zinc', 'dtpa zinc (mg/kg)'),
            fld('exc_aluminium', 'exc. aluminium (meq/100g)'),
            fld('exc_calcium', 'exc. calcium (meq/100g)'),
            fld('exc_magnesium', 'exc. magnesium (meq/100g)'),
            fld('exc_potassium', 'exc. potassium (meq/100g)'),
            fld('exc_sodium', 'exc. sodium (meq/100g)'),
            fld('boron_hot_cacl2', 'boron hot cacl2 (mg/kg)'),
        ]
        wrapper = ExcelWrapper(field_spec, metadata_path, sheet_name=None, header_length=1, column_name_row_index=0)

        return wrapper.get_all()


class BASENCBIContextual(NCBISRAContextual):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/base/metadata/ncbi/']
    name = 'base-ncbi-contextual'
    bioproject_accession = 'PRJNA317932'
