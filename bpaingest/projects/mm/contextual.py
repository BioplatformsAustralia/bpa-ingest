import re
from glob import glob
from ...util import make_logger, one
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper, make_field_definition as fld
from ...ncbi import NCBISRAContextual


logger = make_logger(__name__)


class MarineMicrobesSampleContextual(object):
    # we smash together the tabs, because there is one tab per sample type
    # each BPA ID should have only one entry (if it has one at all)
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/metadata/contextual/2017-12-06/']
    metadata_patterns = [re.compile(r'^.*\.xlsx$')]
    name = 'mm-samplecontextual'
    field_specs = {
        'Coastal water': [
            fld('bpa_id', 'bpa_id', coerce=ingest_utils.extract_bpa_id),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)'),
            fld('longitude', 'longitude (decimal degrees)'),
            fld('depth', 'depth (m)'),
            fld('geo_loc', 'geo_loc (country:subregion)'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('notes', 'notes'),
            fld('ph_level', 'ph level (h2o) (ph)'),
            fld('oxygen_lab', 'oxygen (mol/l) lab'),
            fld('oxygen_ctd', 'oxygen (ml/l) ctd'),
            fld('nitrate_nitrite', 'nitrate/nitrite (mol/l)'),
            fld('phosphate', 'phosphate (mol/l)'),
            fld('ammonium', 'ammonium (mol/l)'),
            fld('total_co2', 'total co2 (mol/kg)'),
            fld('total_alkalinity', 'total alkalinity (mol/kg)'),
            fld('temperature', 'temperature [its-90, deg c]'),
            fld('conductivity', 'conductivity [s/m]'),
            fld('turbidity', 'turbidity (upoly 0, wet labs flnturt)'),
            fld('salinity', 'salinity [psu] laboratory'),
            fld('microbial_abundance', 'microbial abundance (cells per ml)'),
            fld('chlorophyll_a', 'chlorophyll a (g/l)'),
            fld('per_total_carbon', '%total carbon'),
            fld('per_total_inorganc_carbon', '% total inorganc carbon'),
            fld('light_intensity', 'light intensity (lux)'),
        ],
        'Coral': [
            fld('bpa_id', 'bpa_id', coerce=ingest_utils.extract_bpa_id),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)'),
            fld('longitude', 'longitude (decimal degrees)'),
            fld('depth', 'depth (m)'),
            fld('geo_loc', 'geo_loc (country:subregion)'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('host_species', 'host species'),
            fld('notes', 'notes'),
            fld('pulse_amplitude_modulated_fluorometer_measurement', 'pulse amplitude modulated (pam) fluorometer measurement'),
            fld('host_state', 'host state (free text field)'),
            fld('host_abundance', 'host abundance (individuals per m2)'),
        ],
        'Pelagic': [
            fld('bpa_id', 'bpa_id', coerce=ingest_utils.extract_bpa_id),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)'),
            fld('longitude', 'longitude (decimal degrees)'),
            fld('depth', 'depth (m)'),
            fld('geo_loc', 'geo_loc (country:subregion)'),
            fld('sample_site', 'sample site'),
            fld('notes', 'notes'),
            fld('ph_level', 'ph level (h2o) (ph)'),
            fld('oxygen_umol_l_lab', 'oxygen (mol/l) lab'),
            fld('oxygen_umol_kg_ctd', 'oxygen (mol/kg) ctd'),
            fld('oxygen_ml_l_ctd', 'oxygen (ml/l) ctd'),
            fld('silicate', 'silicate (mol/l)'),
            fld('nitrate_nitrite', 'nitrate/nitrite (mol/l)'),
            fld('phosphate', 'phosphate (mol/l)'),
            fld('ammonium', 'ammonium (mol/l)'),
            fld('total_co2', 'total co2 (mol/kg)'),
            fld('total_alkalinity', 'total alkalinity (mol/kg)'),
            fld('temperature', 'temperature [its-90, deg c]'),
            fld('conductivity', 'conductivity [s/m]'),
            fld('fluorescence_wetlab', 'fluorescence, wetlab eco-afl/fl [mg/m^3]'),
            fld('fluorescence', 'fluorescence (au)'),
            fld('transmittance', 'transmittance (%)'),
            fld('turbidity', 'turbidity (nephelometric turbidity units) ctd'),
            fld('density', 'density [density, kg/m^3] ctd'),
            fld('depth_salt_water', 'depth [salt water, m], lat = -27.2'),
            fld('salinity_lab', 'salinity [psu] lab'),
            fld('salinity_ctd', 'salinity [psu] ctd'),
            fld('tss', 'tss [mg/l]'),
            fld('inorganic_fraction', 'inorganic fraction [mg/l]'),
            fld('organic_fraction', 'organic fraction [mg/l]'),
            fld('secchi_depth', 'secchi depth (m)'),
            fld('biomass', 'biomass (mg/m3)'),
            fld('allo', 'allo [mg/m3]'),
            fld('alpha_beta_car', 'alpha_beta_car [mg/m3]'),
            fld('anth', 'anth [mg/m3]'),
            fld('asta', 'asta [mg/m3]'),
            fld('beta_beta_car', 'beta_beta_car [mg/m3]'),
            fld('beta_epi_car', 'beta_epi_car [mg/m3]'),
            fld('but_fuco', 'but_fuco [mg/m3]'),
            fld('cantha', 'cantha [mg/m3]'),
            fld('cphl_a', 'cphl_a [mg/m3]'),
            fld('cphl_b', 'cphl_b [mg/m3]'),
            fld('cphl_c1c2', 'cphl_c1c2 [mg/m3]'),
            fld('cphl_c1', 'cphl_c1 [mg/m3]'),
            fld('cphl_c2', 'cphl_c2 [mg/m3]'),
            fld('cphl_c3', 'cphl_c3 [mg/m3]'),
            fld('cphlide_a', 'cphlide_a [mg/m3]'),
            fld('diadchr', 'diadchr [mg/m3]'),
            fld('diadino', 'diadino [mg/m3]'),
            fld('diato', 'diato [mg/m3]'),
            fld('dino', 'dino [mg/m3]'),
            fld('dv_cphl_a_and_cphl_a', 'dv_cphl_a_and_cphl_a [mg/m3]'),
            fld('dv_cphl_a', 'dv_cphl_a [mg/m3]'),
            fld('dv_cphl_b_and_cphl_b', 'dv_cphl_b_and_cphl_b [mg/m3]'),
            fld('dv_cphl_b', 'dv_cphl_b [mg/m3]'),
            fld('echin', 'echin [mg/m3]'),
            fld('fuco', 'fuco [mg/m3]'),
            fld('gyro', 'gyro [mg/m3]'),
            fld('hex_fuco', 'hex_fuco [mg/m3]'),
            fld('keto_hex_fuco', 'keto_hex_fuco [mg/m3]'),
            fld('lut', 'lut [mg/m3]'),
            fld('lyco', 'lyco [mg/m3]'),
            fld('mg_dvp', 'mg_dvp [mg/m3]'),
            fld('neo', 'neo [mg/m3]'),
            fld('perid', 'perid [mg/m3]'),
            fld('phide_a', 'phide_a [mg/m3]'),
            fld('phytin_a', 'phytin_a [mg/m3]'),
            fld('phytin_b', 'phytin_b [mg/m3]'),
            fld('pras', 'pras [mg/m3]'),
            fld('pyrophide_a', 'pyrophide_a [mg/m3]'),
            fld('pyrophytin_a', 'pyrophytin_a [mg/m3]'),
            fld('viola', 'viola [mg/m3]'),
            fld('zea', 'zea [mg/m3]'),
            fld('dna_extraction_date', 'dna extraction date', coerce=ingest_utils.get_date_isoformat),
            fld('location_code', 'location_code'),
            fld('year', 'year'),
            fld('month', 'month'),
            fld('day', 'day'),
            fld('a16s_comment', 'a16s comment'),
            fld('b16s_comment', 'b16s comment'),
            fld('e18s_comment', 'e18s comment'),
            fld('metagenome_comment', 'metagenome comment'),
            fld('sample_code', 'sample_code'),
            fld('nitrite', 'nitrite'),
            fld('oxygen', 'oxygen (ctd)'),
            fld('ctd_salinity', 'ctd salinity'),
            fld('salinity', 'salinity'),
            fld('extraction_number', 'extraction number'),
            fld('deployment', 'deployment'),
            fld('rp', 'rp'),
            fld('bottom_depth', 'bottom depth'),
            fld('pressure', 'pressure'),
            fld('time', 'time', coerce=ingest_utils.get_time),
            fld('chl_a_epi', 'chl_a_epi'),
            fld('chl_a_allomer', 'chl_a_allomer'),
            fld('zm_delta_sigmat', 'zm (delta.sigmat)'),
            fld('zm__sigmat', 'zm (sigmat)'),
            fld('stratification', 'stratification (zm)'),
            fld('temp_from_ctd_file', '[temp from ctd file]'),
        ],
        'Seaweed': [
            fld('bpa_id', 'bpa_id', coerce=ingest_utils.extract_bpa_id),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)'),
            fld('longitude', 'longitude (decimal degrees)'),
            fld('depth', 'depth (m)'),
            fld('geo_loc', 'geo_loc (country:subregion)'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('host_species', 'host species'),
            fld('notes', 'notes'),
            fld('pulse_amplitude_modulated_pam_fluorometer_measurement', 'pulse amplitude modulated (pam) fluorometer measurement'),
            fld('host_state', 'host state (free text field)'),
            fld('host_abundance', 'host abundance (individuals per m2)'),
        ],
        'Sediment': [
            fld('bpa_id', 'bpa_id', coerce=ingest_utils.extract_bpa_id),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)'),
            fld('longitude', 'longitude (decimal degrees)'),
            fld('depth', 'depth (m)'),
            fld('geo_loc', 'geo_loc (country:subregion)'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('notes', 'notes'),
            fld('per_total_carbon', '%total carbon'),
            fld('per_fine_sediment', '% fine sediment'),
            fld('per_total_nitrogen', '% total nitrogen'),
            fld('per_total_phosphorous', '% total phosphorous'),
            fld('sedimentation_rate', 'sedimentation rate (g /(cm2 x y)r)'),
        ],
        'Sponge': [
            fld('bpa_id', 'bpa_id', coerce=ingest_utils.extract_bpa_id),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)'),
            fld('longitude', 'longitude (decimal degrees)'),
            fld('depth', 'depth (m)'),
            fld('geo_loc', 'geo_loc (country:subregion)'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('host_species', 'host species'),
            fld('notes', 'notes'),
            fld('host_state', 'host state (free text field)'),
            fld('host_abundance', 'host abundance (individuals per m2)'),
        ]
    }

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
                    if val and type(val) is float and val > 0:
                        logger.warning("Positioned in northern hemisphere, inverting: %s / %s" % (row.bpa_id, val))
                        val *= -1
                if field != 'bpa_id':
                    row_meta[field] = val
        return sample_metadata

    def _read_metadata(self, metadata_path):
        rows = []
        for sheet_name, field_spec in sorted(self.field_specs.items()):
            wrapper = ExcelWrapper(
                field_spec,
                metadata_path,
                sheet_name=sheet_name,
                header_length=1,
                column_name_row_index=0,
                additional_context={'sample_type': sheet_name})
            for error in wrapper.get_errors():
                logger.error(error)
            rows += wrapper.get_all()
        return rows

    def filename_metadata(self, *args, **kwargs):
        return {}


class MarineMicrobesNCBIContextual(NCBISRAContextual):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/metadata/ncbi/']
    name = 'base-ncbi-contextual'
    bioproject_accession = 'PRJNA385736'
