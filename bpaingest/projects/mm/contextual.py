import re
from glob import glob
from ...util import make_logger, one
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper
from ...ncbi import NCBISRAContextual


logger = make_logger(__name__)


class MarineMicrobesSampleContextual(object):
    # we smash together the tabs, because there is one tab per sample type
    # each BPA ID should have only one entry (if it has one at all)
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/metadata/contextual/2017-07-27/']
    metadata_patterns = [re.compile(r'^.*\.xlsx$')]
    name = 'mm-samplecontextual'
    field_specs = {
        'Coastal water': [
            ('bpa_id', 'bpa_id', ingest_utils.extract_bpa_id),
            ('date_sampled', 'date sampled (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('time_sampled', 'time sampled (hh:mm)', ingest_utils.get_time),
            ('latitude', 'latitude (decimal degrees)'),
            ('longitude', 'longitude (decimal degrees)'),
            ('depth', 'depth (m)'),
            ('geo_loc', 'geo_loc (country:subregion)'),
            ('sample_site', 'sample site'),
            ('coastal_id', 'coastal_id'),
            ('notes', 'notes'),
            ('ph_level', 'ph level (h2o) (ph)'),
            ('oxygen_lab', 'oxygen (mol/l) lab'),
            ('oxygen_ctd', 'oxygen (ml/l) ctd'),
            ('nitrate_nitrite', 'nitrate/nitrite (mol/l)'),
            ('phosphate', 'phosphate (mol/l)'),
            ('ammonium', 'ammonium (mol/l)'),
            ('total_co2', 'total co2 (mol/kg)'),
            ('total_alkalinity', 'total alkalinity (mol/kg)'),
            ('temperature', 'temperature [its-90, deg c]'),
            ('conductivity', 'conductivity [s/m]'),
            ('turbidity', 'turbidity (upoly 0, wet labs flnturt)'),
            ('salinity', 'salinity [psu] laboratory'),
            ('microbial_abundance', 'microbial abundance (cells per ml)'),
            ('chlorophyll_a', 'chlorophyll a (g/l)'),
            ('per_total_carbon', '%total carbon'),
            ('per_total_inorganc_carbon', '% total inorganc carbon'),
            ('light_intensity', 'light intensity (lux)'),
        ],
        'Coral': [
            ('bpa_id', 'bpa_id', ingest_utils.extract_bpa_id),
            ('date_sampled', 'date sampled (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('time_sampled', 'time sampled (hh:mm)', ingest_utils.get_time),
            ('latitude', 'latitude (decimal degrees)'),
            ('longitude', 'longitude (decimal degrees)'),
            ('depth', 'depth (m)'),
            ('geo_loc', 'geo_loc (country:subregion)'),
            ('sample_site', 'sample site'),
            ('coastal_id', 'coastal_id'),
            ('host_species', 'host species'),
            ('notes', 'notes'),
            ('pulse_amplitude_modulated_fluorometer_measurement', 'pulse amplitude modulated (pam) fluorometer measurement'),
            ('host_state', 'host state (free text field)'),
            ('host_abundance', 'host abundance (individuals per m2)'),
        ],
        'Pelagic': [
            ('bpa_id', 'bpa_id', ingest_utils.extract_bpa_id),
            ('date_sampled', 'date sampled (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('time_sampled', 'time sampled (hh:mm)', ingest_utils.get_time),
            ('latitude', 'latitude (decimal degrees)'),
            ('longitude', 'longitude (decimal degrees)'),
            ('depth', 'depth (m)'),
            ('geo_loc', 'geo_loc (country:subregion)'),
            ('sample_site', 'sample site'),
            ('notes', 'notes'),
            ('ph_level', 'ph level (h2o) (ph)'),
            ('oxygen_umol_l_lab', 'oxygen (mol/l) lab'),
            ('oxygen_umol_kg_ctd', 'oxygen (mol/kg) ctd'),
            ('oxygen_ml_l_ctd', 'oxygen (ml/l) ctd'),
            ('silicate', 'silicate (mol/l)'),
            ('nitrate_nitrite', 'nitrate/nitrite (mol/l)'),
            ('phosphate', 'phosphate (mol/l)'),
            ('ammonium', 'ammonium (mol/l)'),
            ('total_co2', 'total co2 (mol/kg)'),
            ('total_alkalinity', 'total alkalinity (mol/kg)'),
            ('temperature', 'temperature [its-90, deg c]'),
            ('conductivity', 'conductivity [s/m]'),
            ('fluorescence_wetlab', 'fluorescence, wetlab eco-afl/fl [mg/m^3]'),
            ('fluorescence', 'fluorescence (au)'),
            ('transmittance', 'transmittance (%)'),
            ('turbidity', 'turbidity (nephelometric turbidity units) ctd'),
            ('density', 'density [density, kg/m^3] ctd'),
            ('depth_salt_water', 'depth [salt water, m], lat = -27.2'),
            ('salinity_lab', 'salinity [psu] lab'),
            ('salinity_ctd', 'salinity [psu] ctd'),
            ('tss', 'tss [mg/l]'),
            ('inorganic_fraction', 'inorganic fraction [mg/l]'),
            ('organic_fraction', 'organic fraction [mg/l]'),
            ('secchi_depth', 'secchi depth (m)'),
            ('biomass', 'biomass (mg/m3)'),
            ('allo', 'allo [mg/m3]'),
            ('alpha_beta_car', 'alpha_beta_car [mg/m3]'),
            ('anth', 'anth [mg/m3]'),
            ('asta', 'asta [mg/m3]'),
            ('beta_beta_car', 'beta_beta_car [mg/m3]'),
            ('beta_epi_car', 'beta_epi_car [mg/m3]'),
            ('but_fuco', 'but_fuco [mg/m3]'),
            ('cantha', 'cantha [mg/m3]'),
            ('cphl_a', 'cphl_a [mg/m3]'),
            ('cphl_b', 'cphl_b [mg/m3]'),
            ('cphl_c1c2', 'cphl_c1c2 [mg/m3]'),
            ('cphl_c1', 'cphl_c1 [mg/m3]'),
            ('cphl_c2', 'cphl_c2 [mg/m3]'),
            ('cphl_c3', 'cphl_c3 [mg/m3]'),
            ('cphlide_a', 'cphlide_a [mg/m3]'),
            ('diadchr', 'diadchr [mg/m3]'),
            ('diadino', 'diadino [mg/m3]'),
            ('diato', 'diato [mg/m3]'),
            ('dino', 'dino [mg/m3]'),
            ('dv_cphl_a_and_cphl_a', 'dv_cphl_a_and_cphl_a [mg/m3]'),
            ('dv_cphl_a', 'dv_cphl_a [mg/m3]'),
            ('dv_cphl_b_and_cphl_b', 'dv_cphl_b_and_cphl_b [mg/m3]'),
            ('dv_cphl_b', 'dv_cphl_b [mg/m3]'),
            ('echin', 'echin [mg/m3]'),
            ('fuco', 'fuco [mg/m3]'),
            ('gyro', 'gyro [mg/m3]'),
            ('hex_fuco', 'hex_fuco [mg/m3]'),
            ('keto_hex_fuco', 'keto_hex_fuco [mg/m3]'),
            ('lut', 'lut [mg/m3]'),
            ('lyco', 'lyco [mg/m3]'),
            ('mg_dvp', 'mg_dvp [mg/m3]'),
            ('neo', 'neo [mg/m3]'),
            ('perid', 'perid [mg/m3]'),
            ('phide_a', 'phide_a [mg/m3]'),
            ('phytin_a', 'phytin_a [mg/m3]'),
            ('phytin_b', 'phytin_b [mg/m3]'),
            ('pras', 'pras [mg/m3]'),
            ('pyrophide_a', 'pyrophide_a [mg/m3]'),
            ('pyrophytin_a', 'pyrophytin_a [mg/m3]'),
            ('viola', 'viola [mg/m3]'),
            ('zea', 'zea [mg/m3]'),
            ('dna_extraction_date', 'dna extraction date', ingest_utils.get_date_isoformat),
            ('location_code', 'location_code'),
            ('year', 'year'),
            ('month', 'month'),
            ('day', 'day'),
            ('a16s_comment', 'a16s comment'),
            ('b16s_comment', 'b16s comment'),
            ('e18s_comment', 'e18s comment'),
            ('metagenome_comment', 'metagenome comment'),
            ('sample_code', 'sample_code'),
            ('nitrite', 'nitrite'),
            ('oxygen', 'oxygen (ctd)'),
            ('ctd_salinity', 'ctd salinity'),
            ('salinity', 'salinity'),
            ('extraction_number', 'extraction number'),
            ('deployment', 'deployment'),
            ('rp', 'rp'),
            ('bottom_depth', 'bottom depth'),
            ('pressure', 'pressure'),
            ('time', 'time', ingest_utils.get_time),
            ('chl_a_epi', 'chl_a_epi'),
            ('chl_a_allomer', 'chl_a_allomer'),
            ('zm_delta_sigmat', 'zm (delta.sigmat)'),
            ('zm__sigmat', 'zm (sigmat)'),
            ('stratification', 'stratification (zm)'),
            ('temp_from_ctd_file', '[temp from ctd file]'),
        ],
        'Seaweed': [
            ('bpa_id', 'bpa_id', ingest_utils.extract_bpa_id),
            ('date_sampled', 'date sampled (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('time_sampled', 'time sampled (hh:mm)', ingest_utils.get_time),
            ('latitude', 'latitude (decimal degrees)'),
            ('longitude', 'longitude (decimal degrees)'),
            ('depth', 'depth (m)'),
            ('geo_loc', 'geo_loc (country:subregion)'),
            ('sample_site', 'sample site'),
            ('coastal_id', 'coastal_id'),
            ('host_species', 'host species'),
            ('notes', 'notes'),
            ('pulse_amplitude_modulated_pam_fluorometer_measurement', 'pulse amplitude modulated (pam) fluorometer measurement'),
            ('host_state', 'host state (free text field)'),
            ('host_abundance', 'host abundance (individuals per m2)'),
        ],
        'Sediment': [
            ('bpa_id', 'bpa_id', ingest_utils.extract_bpa_id),
            ('date_sampled', 'date sampled (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('time_sampled', 'time sampled (hh:mm)', ingest_utils.get_time),
            ('latitude', 'latitude (decimal degrees)'),
            ('longitude', 'longitude (decimal degrees)'),
            ('depth', 'depth (m)'),
            ('geo_loc', 'geo_loc (country:subregion)'),
            ('sample_site', 'sample site'),
            ('coastal_id', 'coastal_id'),
            ('notes', 'notes'),
            ('per_total_carbon', '%total carbon'),
            ('per_fine_sediment', '% fine sediment'),
            ('per_total_nitrogen', '% total nitrogen'),
            ('per_total_phosphorous', '% total phosphorous'),
            ('sedimentation_rate', 'sedimentation rate (g /(cm2 x y)r)'),
        ],
        'Sponge': [
            ('bpa_id', 'bpa_id', ingest_utils.extract_bpa_id),
            ('date_sampled', 'date sampled (yyyy-mm-dd)', ingest_utils.get_date_isoformat),
            ('time_sampled', 'time sampled (hh:mm)', ingest_utils.get_time),
            ('latitude', 'latitude (decimal degrees)'),
            ('longitude', 'longitude (decimal degrees)'),
            ('depth', 'depth (m)'),
            ('geo_loc', 'geo_loc (country:subregion)'),
            ('sample_site', 'sample site'),
            ('coastal_id', 'coastal_id'),
            ('host_species', 'host species'),
            ('notes', 'notes'),
            ('host_state', 'host state (free text field)'),
            ('host_abundance', 'host abundance (individuals per m2)'),
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
                    if val and val > 0:
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
            rows += wrapper.get_all()
        return rows

    def filename_metadata(self, *args, **kwargs):
        return {}


class MarineMicrobesNCBIContextual(NCBISRAContextual):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/metadata/ncbi/']
    name = 'base-ncbi-contextual'
    bioproject_accession = 'PRJNA385736'
