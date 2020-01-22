import datetime
import re
from glob import glob
from ...libs import ingest_utils
from ...libs.excel_wrapper import (
    ExcelWrapper,
    SkipColumn as skip,
    FieldDefinition,
    make_field_definition as fld)
from ...util import make_logger, one
from ...ncbi import NCBISRAContextual
from collections import defaultdict
from .vocabularies import (
    AustralianSoilClassificationVocabulary,
    BroadVegetationTypeVocabulary,
    CropRotationClassification,
    EcologicalZoneVocabulary,
    FAOSoilClassificationVocabulary,
    HorizonClassificationVocabulary,
    LandUseVocabulary,
    ProfilePositionVocabulary,
    SoilColourVocabulary,
    TillageClassificationVocabulary)


logger = make_logger(__name__)

CHEM_MIN_SENTINAL_VALUE = 0.0001


class NotInVocabulary(Exception):
    pass


class BaseOntologyEnforcement:
    def __init__(self):
        self.norm_to_term = self._build_terms()

    def _build_terms(self):
        terms = [t[0] for t in self.vocabulary]
        return dict((BaseOntologyEnforcement._normalise(x), x) for x in terms)

    @classmethod
    def _normalise(cls, s):
        s = s.lower()
        s = s.replace(" ", "")
        s = s.replace("&", "and")
        s = s.replace('-', "")
        return s

    def get(self, term):
        """
        returns the term, as found in the list of appropriate terms,
        or raises NotInVocabulary
        """
        if term is None:
            return ''
        norm = self._normalise(term)
        if not norm:
            return ''
        elif norm in self.norm_to_term:
            return self.norm_to_term[norm]
        else:
            raise NotInVocabulary(term)


class BroadLandUseEnforcement(BaseOntologyEnforcement):
    vocabulary = LandUseVocabulary


class AustralianSoilClassificationEnforcement(BaseOntologyEnforcement):
    vocabulary = AustralianSoilClassificationVocabulary

    def __init__(self):
        super().__init__()
        self.norm_to_term[self._normalise('Tenosol')] = 'Tenosols'
        self.norm_to_term[self._normalise('Chromosol')] = 'Chromosols'
        self.norm_to_term[self._normalise('Hydrosol')] = 'Hydrosols'


class ProfilePositionEnforcement(BaseOntologyEnforcement):
    vocabulary = ProfilePositionVocabulary


class BroadVegetationTypeEnforement(BaseOntologyEnforcement):
    vocabulary = BroadVegetationTypeVocabulary


class FAOSoilClassificationEnforcement(BaseOntologyEnforcement):
    vocabulary = FAOSoilClassificationVocabulary

    def __init__(self):
        super().__init__()
        self.norm_to_term[self._normalise('Tenosol')] = 'Tenosols'
        self.norm_to_term[self._normalise('Cambisol')] = 'Cambisols'


class SoilColourEnforcement(BaseOntologyEnforcement):
    vocabulary = SoilColourVocabulary

    def _build_terms(self):
        terms = [t[1] for t in self.vocabulary]
        return dict((BaseOntologyEnforcement._normalise(x), x) for x in terms)


class HorizonClassificationEnforcement(BaseOntologyEnforcement):
    vocabulary = HorizonClassificationVocabulary

    def get(self, term):
        if term is None:
            return ''
        # codes are single characters. we check each character
        # against the vocabulary; if it's not in there, we chuck it out
        terms = []
        for c in term:
            norm = self._normalise(c)
            if not norm or norm not in self.norm_to_term:
                continue
            terms.append(self.norm_to_term[norm])
        return ','.join(sorted(terms))


class EcologicalZoneEnforcement(BaseOntologyEnforcement):
    vocabulary = EcologicalZoneVocabulary

    def __init__(self):
        super().__init__()
        self.norm_to_term[self._normalise('Tenosol')] = 'Tenosols'
        self.norm_to_term[self._normalise('Mediterranian')] = 'Mediterranean'
        self.norm_to_term[self._normalise('Wet Tropics')] = 'Tropical (wet)'
        self.norm_to_term[self._normalise('Other (polar)')] = 'Polar'


class TillageClassificationEnforcement(BaseOntologyEnforcement):
    vocabulary = TillageClassificationVocabulary

    def get(self, term):
        # take first part of string which is the tillage and leave out description
        if term is None:
            return ''
        first_part = term.split(":")[0]
        return super().get(first_part)


class CropRotationEnforcement(BaseOntologyEnforcement):
    vocabulary = CropRotationClassification

    def _build_terms(self):
        terms = [t for t in self.vocabulary]
        return dict((BaseOntologyEnforcement._normalise(x), x) for x in terms)


class LandUseEnforcement(BaseOntologyEnforcement):
    def __init__(self):
        self.tree = self._build_tree()

    def _build_tree(self):
        def expand_tree(values, tree, prefix=[]):
            # some of the names are actually a tree path in themselves
            name = [t.strip() for t in values[0].split('-')]
            path = prefix + name
            norm_path = tuple([self._normalise(t) for t in path])
            tree[norm_path] = ' - '.join(path)
            for value in values[1:]:
                if isinstance(value, tuple):
                    # a tuple is a sub-tree which we recurse into
                    if value:
                        expand_tree(value, tree, prefix=path)
                else:
                    # a string is a fellow leaf-node of the parent
                    expand_tree((value,), tree, prefix=prefix)

        tree = {}
        for subtree in LandUseVocabulary:
            expand_tree(subtree, tree)
        return tree

    def get(self, original):
        if original is None:
            return ''

        query = tuple([t for t in [self._normalise(t) for t in original.split('-')] if t])

        if len(query) == 0:
            return ''

        # tree contains all fully expanded paths through the classification tree,
        # as tuples, and the values in the tree are the string representation of these
        # fully expanded forms. tuples have been run through normalisation.

        matches = []
        for code, classification in self.tree.items():
            if code[-len(query):] == query:
                matches.append(code)

        matches.sort(key=lambda m: len(m))
        if matches:
            return self.tree[matches[0]]
        else:
            raise NotInVocabulary(original)


def fix_sometimes_date(val):
    "mix of dates and free-text, make into strings"
    if isinstance(val, datetime.date) or isinstance(val, datetime.datetime):
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


class BASENCBIContextual(NCBISRAContextual):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/base/metadata/ncbi/']
    name = 'base-ncbi-contextual'
    bioproject_accession = 'PRJNA317932'


class AustralianMicrobiomeSampleContextual(object):
    # we smash together the tabs, because there is one tab per sample type
    # each BPA ID should have only one entry (if it has one at all)
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/amd/metadata/contextual/2020-01-22/']
    metadata_patterns = [re.compile(r'^.*\.xlsx$')]
    name = 'amd-samplecontextual'
    field_specs = {
        'Coastal water': [
            fld('sample_id', 'bpa_id', coerce=ingest_utils.extract_ands_id),
            skip('ncbi_submission'),
            skip('id'),
            skip('ncbi sample accession'),
            fld('organism', 'organism'),
            skip('tax id'),
            fld('samplename_depth', 'samplename_depth'),
            skip('ncbi bioproject'),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('depth', 'depth (m)', units='m', coerce=ingest_utils.get_clean_number),
            fld('geo_loc', 'geo_loc (country:subregion)', units='country:subregion'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('notes', 'notes'),
            fld('ph_level', 'ph level (h2o) (ph)', units='h2o', coerce=ingest_utils.get_clean_number),
            fld('oxygen_lab', 'oxygen (μmol/l) lab', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('oxygen_ctd_coastal_water', 'oxygen (ml/l) ctd', units='ml/l', coerce=ingest_utils.get_clean_number),
            fld('nitrate_nitrite', 'nitrate/nitrite (μmol/l)', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('phosphate', 'phosphate (μmol/l)', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('ammonium', 'ammonium (μmol/l)', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('total_co2', 'total co2 (μmol/kg)', units='μmol/kg', coerce=ingest_utils.get_clean_number),
            fld('total_alkalinity', 'total alkalinity (μmol/kg)', units='μmol/kg', coerce=ingest_utils.get_clean_number),
            fld('temperature',
                'temperature [its-90, deg c]',
                units='its-90, deg c',
                coerce=ingest_utils.get_clean_number),
            fld('conductivity_sm', 'conductivity [s/m]', units='s/m', coerce=ingest_utils.get_clean_number),
            fld('turbidity',
                'turbidity (upoly 0, wet labs flnturt)',
                units='upoly 0, wet labs flnturt',
                coerce=ingest_utils.get_clean_number),
            fld('salinity_lab', 'salinity [psu] laboratory', units='psu', coerce=ingest_utils.get_clean_number),
            fld('microbial_abundance', 'microbial abundance (cells per ml)',
                units='cells per ml', coerce=ingest_utils.get_clean_number),
            fld('chlorophyll_a', 'chlorophyll a (μg/l)', units='μg/l', coerce=ingest_utils.get_clean_number),
            fld('total_carbon', '%total carbon', units='%', coerce=ingest_utils.get_clean_number),
            fld('total_inorganc_carbon', '% total inorganc carbon', units='%', coerce=ingest_utils.get_clean_number),
            fld('light_intensity', 'light intensity (lux)', units='lux', coerce=ingest_utils.get_clean_number),
            fld('tss', 'tss (mg/l)', units='mg/l', coerce=ingest_utils.get_clean_number),
            fld('sio2', 'sio2 (µmol/l)', units='µmol/l', coerce=ingest_utils.get_clean_number),
            fld('no2', 'no2 (µmol/l)', units='µmol/l', coerce=ingest_utils.get_clean_number),
            fld('poc', 'poc (µmol/l)', units='µmol/l', coerce=ingest_utils.get_clean_number),
            fld('pn', 'pn (µmol/l)', units='µmol/l', coerce=ingest_utils.get_clean_number),
            fld('npoc', 'npoc (mg/l)', units='mg/l', coerce=ingest_utils.get_clean_number),
            fld('npic', 'npic (mg/l)', units='mg/l', coerce=ingest_utils.get_clean_number),
        ],
        'Coral': [
            fld('sample_id', 'bpa_id', coerce=ingest_utils.extract_ands_id),
            skip('ncbi_submission'),
            skip('id'),
            skip('ncbi sample accession'),
            fld('organism', 'organism'),
            skip('tax id'),
            fld('samplename_depth', 'samplename_depth'),
            skip('ncbi bioproject'),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('depth', 'depth (m)', units='m', coerce=ingest_utils.get_clean_number),
            fld('geo_loc', 'geo_loc (country:subregion)', units='country:subregion'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('host_species', 'host species'),
            fld('notes', 'notes'),
            fld('pam_fluorometer', 'pulse amplitude modulated (pam) fluorometer measurement',
                units='pam', coerce=ingest_utils.get_clean_number),
            fld('host_state', 'host state (free text field)'),
            fld('host_abundance', 'host abundance (individuals per m2)',
                units='individuals per m2', coerce=ingest_utils.get_clean_number),
        ],
        'Pelagic_Public': [
            fld('sample_id', 'bpa_id', coerce=ingest_utils.extract_ands_id),
            skip('ncbi_submission'),
            skip('id'),
            skip('ncbi sample accession'),
            fld('organism', 'organism'),
            skip('tax id'),
            fld('samplename_depth', 'samplename_depth'),
            skip('ncbi bioproject'),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('depth', 'depth (m)', units='m', coerce=ingest_utils.get_clean_number),
            fld('geo_loc', 'geo_loc (country:subregion)', units='country:subregion'),
            fld('sample_site', 'sample site'),
            fld('nrs_location_code_voyage_code', 'nrs_location_code; voyage_code'),
            fld('nrs_trip_code', 'nrs_trip_code'),
            fld('nrs_sample_code', 'nrs_sample_code'),
            fld('notes', 'notes'),
            fld('ph_level', 'ph level (h2o) (ph)', units='h2o', coerce=ingest_utils.get_clean_number),
            fld('fluorescence', 'fluorescence (au)', units='au', coerce=ingest_utils.get_clean_number),
            fld('transmittance', 'transmittance (%)', units='%', coerce=ingest_utils.get_clean_number),
            fld('secchi_depth', 'secchi depth (m)', units='m', coerce=ingest_utils.get_clean_number),
            fld('bottom_depth', 'bottom depth', coerce=ingest_utils.get_clean_number),
            fld('pressure_bottle', 'pressure bottle', coerce=ingest_utils.get_clean_number),
            fld('temperature_ctd', 'temperature: ctd [its-90, deg c]',
                units='its-90, deg c', coerce=ingest_utils.get_clean_number),
            fld('salinity_ctd', 'salinity [psu] ctd', units='psu', coerce=ingest_utils.get_clean_number),
            fld('oxygen_ctd_pelagic', 'oxygen (μmol/kg) ctd', units='μmol/kg', coerce=ingest_utils.get_clean_number),
            fld('density_ctd',
                'density [density, kg/m^3] ctd',
                units='density, kg/m^3',
                coerce=ingest_utils.get_clean_number),
            fld('turbidity_ctd', 'turbidity (nephelometric turbidity units) ctd',
                units='nephelometric turbidity units', coerce=ingest_utils.get_clean_number),
            fld('chlf_ctd', 'chlf: ctd', coerce=ingest_utils.get_clean_number),
            fld('silicate', 'silicate (μmol/l)', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('nitrate_nitrite', 'nitrate/nitrite (μmol/l)', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('nitrite', 'nitrite (μmol/l)', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('phosphate', 'phosphate (μmol/l)', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('ammonium', 'ammonium (μmol/l)', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('salinity_lab', 'salinity [psu] lab', units='psu', coerce=ingest_utils.get_clean_number),
            fld('oxygen_lab', 'oxygen (μmol/l) lab', units='μmol/l', coerce=ingest_utils.get_clean_number),
            fld('total_co2', 'total co2 (μmol/kg)', units='μmol/kg', coerce=ingest_utils.get_clean_number),
            fld('total_alkalinity', 'total alkalinity (μmol/kg)', units='μmol/kg', coerce=ingest_utils.get_clean_number),
            fld('tss', 'tss [mg/l]', units='mg/l', coerce=ingest_utils.get_clean_number),
            fld('inorganic_fraction', 'inorganic fraction [mg/l]', units='mg/l', coerce=ingest_utils.get_clean_number),
            fld('organic_fraction', 'organic fraction [mg/l]', units='mg/l', coerce=ingest_utils.get_clean_number),
            fld('allo', 'allo [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('alpha_beta_car', 'alpha_beta_car [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('anth', 'anth [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('asta', 'asta [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('beta_beta_car', 'beta_beta_car [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('beta_epi_car', 'beta_epi_car [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('but_fuco', 'but_fuco [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('cantha', 'cantha [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('cphl_a', 'cphl_a [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('cphl_b', 'cphl_b [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('cphl_c1c2', 'cphl_c1c2 [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('cphl_c1', 'cphl_c1 [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('cphl_c2', 'cphl_c2 [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('cphl_c3', 'cphl_c3 [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('cphlide_a', 'cphlide_a [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('diadchr', 'diadchr [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('diadino', 'diadino [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('diato', 'diato [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('dino', 'dino [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('dv_cphl_a_and_cphl_a', 'dv_cphl_a_and_cphl_a [mg/m3]',
                units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('dv_cphl_a', 'dv_cphl_a [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('dv_cphl_b_and_cphl_b', 'dv_cphl_b_and_cphl_b [mg/m3]',
                units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('dv_cphl_b', 'dv_cphl_b [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('echin', 'echin [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('fuco', 'fuco [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('gyro', 'gyro [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('hex_fuco', 'hex_fuco [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('keto_hex_fuco', 'keto_hex_fuco [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('lut', 'lut [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('lyco', 'lyco [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('mg_dvp', 'mg_dvp [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('neo', 'neo [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('perid', 'perid [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('phide_a', 'phide_a [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('phytin_a', 'phytin_a [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('phytin_b', 'phytin_b [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('pras', 'pras [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('pyrophide_a', 'pyrophide_a [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('pyrophytin_a', 'pyrophytin_a [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('viola', 'viola [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
            fld('zea', 'zea [mg/m3]', units='mg/m3', coerce=ingest_utils.get_clean_number),
        ],
        'Seagrass': [
            fld('sample_id', 'bpa_id', coerce=ingest_utils.extract_ands_id),
            skip('ncbi_submission'),
            skip('id'),
            skip('ncbi sample accession'),
            fld('organism', 'organism'),
            skip('tax id'),
            fld('samplename_depth', 'samplename_depth'),
            skip('ncbi bioproject'),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('depth', 'depth (m)', units='m', coerce=ingest_utils.get_clean_number),
            fld('geo_loc', 'geo_loc (country:subregion)', units='country:subregion'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('host_species', 'host species'),
            fld('notes', 'notes'),
            fld('pam_fluorometer', 'pulse amplitude modulated (pam) fluorometer measurement',
                units='pam', coerce=ingest_utils.get_clean_number),
            fld('host_state', 'host state (free text field)'),
            fld('host_abundance', 'host abundance (individuals per m2)',
                units='individuals per m2', coerce=ingest_utils.get_clean_number),
            fld('light_intensity_surface',
                'light intensity (surface) [µmol/m²/s¯¹]',
                units='µmol/m²/s¯¹',
                coerce=ingest_utils.get_clean_number),
            fld('light_intensity_meadow',
                'light intensity (meadow) [µmol/m²/s¯¹]',
                units='µmol/m²/s¯¹',
                coerce=ingest_utils.get_clean_number),
        ],
        'Seaweed': [
            fld('sample_id', 'bpa_id', coerce=ingest_utils.extract_ands_id),
            skip('ncbi_submission'),
            skip('id'),
            skip('ncbi sample accession'),
            fld('organism', 'organism'),
            skip('tax id'),
            fld('samplename_depth', 'samplename_depth'),
            skip('ncbi bioproject'),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('depth', 'depth (m)', units='m', coerce=ingest_utils.get_clean_number),
            fld('geo_loc', 'geo_loc (country:subregion)', units='country:subregion'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('host_species', 'host species'),
            fld('notes', 'notes'),
            fld('pam_fluorometer', 'pulse amplitude modulated (pam) fluorometer measurement',
                units='pam', coerce=ingest_utils.get_clean_number),
            fld('host_state', 'host state (free text field)'),
            fld('average_host_abundance', 'average host abundance (% of individuals per m2)',
                units='%', coerce=ingest_utils.get_clean_number),
            fld('host_abundance_seaweed', 'host abundance (mean number ind per m2 +/- se)',
                units='mean number ind per m2 +/- se', coerce=ingest_utils.get_clean_number),
            fld('length', 'length(cm)', units='cm', coerce=ingest_utils.get_clean_number),
            fld('fouling', 'fouling (%)', units='%', coerce=ingest_utils.get_clean_number),
            fld('fouling_organisms', 'fouling_organisms'),
            fld('grazing_number', 'grazing_number', coerce=ingest_utils.get_clean_number),
            fld('grazing', 'grazing (%)', units='%', coerce=ingest_utils.get_clean_number),
            fld('bleaching', 'bleaching (%)', units='%', coerce=ingest_utils.get_clean_number),
            fld('touching_organisms', 'touching_organisms'),
            fld('information', 'information'),
        ],
        'Sediment': [
            fld('sample_id', 'bpa_id', coerce=ingest_utils.extract_ands_id),
            skip('ncbi_submission'),
            skip('id'),
            skip('ncbi sample accession'),
            fld('organism', 'organism'),
            skip('tax id'),
            fld('samplename_depth', 'samplename_depth'),
            skip('ncbi bioproject'),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('depth', 'depth (m)', units='m', coerce=ingest_utils.get_clean_number),
            fld('geo_loc', 'geo_loc (country:subregion)', units='country:subregion'),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('notes', 'notes'),
            fld('total_carbon', '%total carbon', units='%', coerce=ingest_utils.get_clean_number),
            fld('fine_sediment', '% fine sediment', units='%', coerce=ingest_utils.get_clean_number),
            fld('total_nitrogen', '% total nitrogen', units='%', coerce=ingest_utils.get_clean_number),
            fld('total_phosphorous', '% total phosphorous', units='%', coerce=ingest_utils.get_clean_number),
            fld('sedimentation_rate', 'sedimentation rate (g /(cm2 x y)r)',
                units='g /(cm2 x y', coerce=ingest_utils.get_clean_number),
        ],
        'Soil': [
            fld('sample_id', 'sample_id', coerce=ingest_utils.extract_ands_id),
            fld('date_sampled', 'date sampled', coerce=ingest_utils.get_date_isoformat),
            fld('latitude', 'latitude', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('depth', 'depth', units='m', coerce=ingest_utils.get_clean_number),
            fld('sample_storage_method', 'sample_storage_method'),
            fld('geo_loc', 'geo_loc', units='country:subregion'),
            fld('location_description', 'location description'),
            fld('broad_land_use', 'broad land use'),
            fld('detailed_land_use', 'detailed land use'),
            fld('general_ecological_zone', 'general ecological zone'),
            fld('vegetation_type', 'vegetation type'),
            fld('elevation', 'elevation ()', coerce=ingest_utils.get_clean_number),
            fld('slope', 'slope (%)', units='%', coerce=ingest_utils.get_clean_number),
            fld('slope_aspect', 'slope aspect (direction or degrees; e.g., nw or 315°)',
                units='direction or degrees; e.g., nw or 315°', coerce=ingest_utils.get_clean_number),
            fld('australian_soil_classification', 'australian soil classification controlled vocab (6)'),
            fld('soil_moisture', 'soil moisture (%)', units='%', coerce=ingest_utils.get_clean_number),
            fld('color', 'color controlled vocab (10)'),
            fld('gravel', 'gravel (%)- ( >2.0 mm)', units='%', coerce=ingest_utils.get_clean_number),
            fld('texture', 'texture ()'),
            fld('course_sand', 'course sand (%) (200-2000 µm)', units='%', coerce=ingest_utils.get_clean_number),
            fld('fine_sand', 'fine sand (%) - (20-200 µm)', units='%', coerce=ingest_utils.get_clean_number),
            fld('sand', 'sand (%)', units='%', coerce=ingest_utils.get_clean_number),
            fld('silt', 'silt  (%) (2-20 µm)', units='%', coerce=ingest_utils.get_clean_number),
            fld('clay', 'clay (%) (<2 µm)', units='%', coerce=ingest_utils.get_clean_number),
            fld('ammonium_nitrogen', 'ammonium nitrogen (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
            fld('nitrate_nitrogen', 'nitrate nitrogen (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
            fld('phosphorus_colwell', 'phosphorus colwell (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
            fld('potassium_colwell', 'potassium colwell (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
            fld('sulphur', 'sulphur (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
            fld('organic_carbon', 'organic carbon (%)', units='%', coerce=ingest_utils.get_clean_number),
            fld('conductivity_dsm', 'conductivity (ds/m)', units='ds/m', coerce=ingest_utils.get_clean_number),
            fld('ph_level_cacl2', 'ph level (cacl2) (ph)', units='cacl2', coerce=ingest_utils.get_clean_number),
            fld('ph_level_h2o', 'ph level (h2o) (ph)', units='h2o', coerce=ingest_utils.get_clean_number),
            fld('dtpa_copper', 'dtpa copper (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
            fld('dtpa_iron', 'dtpa iron (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
            fld('dtpa_manganese', 'dtpa manganese (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
            fld('dtpa_zinc', 'dtpa zinc (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
            fld('exc_aluminium', 'exc. aluminium (meq/100g)', units='meq/100g', coerce=ingest_utils.get_clean_number),
            fld('exc_calcium', 'exc. calcium (meq/100g)', units='meq/100g', coerce=ingest_utils.get_clean_number),
            fld('exc_magnesium', 'exc. magnesium (meq/100g)', units='meq/100g', coerce=ingest_utils.get_clean_number),
            fld('exc_potassium', 'exc. potassium (meq/100g)', units='meq/100g', coerce=ingest_utils.get_clean_number),
            fld('exc_sodium', 'exc. sodium (meq/100g)', units='meq/100g', coerce=ingest_utils.get_clean_number),
            fld('boron_hot_cacl2', 'boron hot cacl2 (mg/kg)', units='mg/kg', coerce=ingest_utils.get_clean_number),
        ],
        'Sponge': [
            fld('sample_id', 'bpa_id', coerce=ingest_utils.extract_ands_id),
            skip('ncbi_submission'),
            skip('id'),
            skip('ncbi sample accession'),
            fld('organism', 'organism'),
            skip('tax id'),
            fld('samplename_depth', 'samplename_depth'),
            skip('ncbi bioproject'),
            fld('date_sampled', 'date sampled (yyyy-mm-dd)', coerce=ingest_utils.get_date_isoformat),
            fld('time_sampled', 'time sampled (hh:mm)', coerce=ingest_utils.get_time),
            fld('latitude', 'latitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('longitude', 'longitude (decimal degrees)', units='decimal degrees', coerce=ingest_utils.get_clean_number),
            fld('depth', 'depth (m)', units='m', coerce=ingest_utils.get_clean_number),
            fld('geo_loc', 'geo_loc (country:subregion)', units='country:subregion', coerce=ingest_utils.get_clean_number),
            fld('sample_site', 'sample site'),
            fld('coastal_id', 'coastal_id'),
            fld('host_species', 'host species'),
            fld('notes', 'notes'),
            fld('host_state', 'host state (free text field)'),
            fld('host_abundance', 'host abundance (individuals per m2)',
                units='individuals per m2', coerce=ingest_utils.get_clean_number),
        ]
    }
    ontology_cleanups = {
        'Soil': {
            # Seems to have been removed, FIXME query with AB
            # 'horizon_classification': HorizonClassificationEnforcement(),
            'broad_land_use': LandUseEnforcement(),
            'detailed_land_use': LandUseEnforcement(),
            'general_ecological_zone': EcologicalZoneEnforcement(),
            'vegetation_type': BroadVegetationTypeEnforement(),
            # Seems to have been removed, FIXME query with AB
            # 'profile_position': ProfilePositionEnforcement(),
            'australian_soil_classification': AustralianSoilClassificationEnforcement(),
            # Seems to have been removed, FIXME query with AB
            # 'fao_soil_classification': FAOSoilClassificationEnforcement(),
            # Seems to have been removed, FIXME query with AB
            # 'tillage': TillageClassificationEnforcement(),
            'color': SoilColourEnforcement(),
            # Seems to have been removed, FIXME query with AB
            # 'crop_rotation_1yr_since_present': CropRotationEnforcement(),
            # 'crop_rotation_2yrs_since_present': CropRotationEnforcement(),
            # 'crop_rotation_3yrs_since_present': CropRotationEnforcement(),
            # 'crop_rotation_4yrs_since_present': CropRotationEnforcement(),
            # 'crop_rotation_5yrs_since_present': CropRotationEnforcement(),
        }
    }

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.environment_ontology_errors = defaultdict(set)
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

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
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(sample_id)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.sample_id is None:
                continue
            if row.sample_id in sample_metadata:
                raise Exception("Metadata invalid, duplicate sample ID {} in row {}".format(
                    row.sample_id, row))
            assert(row.sample_id not in sample_metadata)
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                val = getattr(row, field)
                if field != 'sample_id':
                    row_meta[field] = val
            ontology_cleanups = self.ontology_cleanups.get(row_meta['environment'])
            if ontology_cleanups is not None:
                for cleanup_name, enforcer in ontology_cleanups.items():
                    try:
                        row_meta[cleanup_name] = enforcer.get(row_meta[cleanup_name])
                    except NotInVocabulary as e:
                        self.environment_ontology_errors[(row_meta['environment'], cleanup_name)].add(
                            e.args[0])
                        del row_meta[cleanup_name]
        return sample_metadata

    @staticmethod
    def environment_for_sheet(sheet_name):
        return 'Soil' if sheet_name == 'Soil' else 'Marine'

    def _read_metadata(self, metadata_path):
        rows = []
        for sheet_name, field_spec in sorted(self.field_specs.items()):
            wrapper = ExcelWrapper(
                field_spec,
                metadata_path,
                sheet_name=sheet_name,
                header_length=1,
                column_name_row_index=0,
                suggest_template=True,
                additional_context={
                    'sample_type': sheet_name,
                    'environment': self.environment_for_sheet(sheet_name)
                })
            for error in wrapper.get_errors():
                logger.error(error)
            rows += wrapper.get_all()
        return rows

    def filename_metadata(self, *args, **kwargs):
        return {}


class MarineMicrobesNCBIContextual(NCBISRAContextual):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/metadata/ncbi/']
    name = 'mm-ncbi-contextual'
    bioproject_accession = 'PRJNA385736'
