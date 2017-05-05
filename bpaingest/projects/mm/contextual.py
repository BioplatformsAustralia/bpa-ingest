import os
import re
from glob import glob
from ...util import csv_to_named_tuple, make_logger
from ...libs.ingest_utils import get_date_isoformat, extract_bpa_id


logger = make_logger(__name__)

METADATA_BASE = 'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/metadata/contextual'


class MarineMicrobesSampleContextual(object):
    # each BPA_ID will have precisely /one/ or /zero/ rows
    metadata_urls = [
        METADATA_BASE + '/coastal_water/',
        METADATA_BASE + '/coral/',
        METADATA_BASE + '/open_water/',
        METADATA_BASE + '/pelagic/',
        METADATA_BASE + '/seaweed/',
        METADATA_BASE + '/sediment/',
        METADATA_BASE + '/sponge/',
    ]
    metadata_patterns = [re.compile(r'^.*\.csv$')]
    name = 'mm-samplecontextual'

    def __init__(self, path):
        self.all_headers, self.data = self._read_csv(path)

    def _read_csv(self, path):
        header_corrections = {
            'host_abundance_individuals_per_m2': 'host_abundance',
            'host_state_free_text_field': 'host_state',
            'host_state_free_text_field': 'host_state',
            'pulse_amplitude_modulated_pam': 'pam',
            'note': 'notes',
            'fluorescence_wetlab_eco': 'fluorescence',
        }
        # remove units from the attribute names
        unit_regexps = [
            r'_upoly_0_wet_labs_flnturt$',
            r'_g_cm2_x_yr$',
            r'_mgm3$',
            r'_m$',
            r'_moll$',
            r'_per_ml$',
            r'_mgl$',
            r'_kgm3$',
            r'_sm$',
            r'_h20$',
            r'_mgl$',
            r'_gl$',
            r'_lux$',
            r'_aflfl$',
            r'_deg_c$',
            r'_molkg$',
        ]

        def get_header_name(hdr):
            if hdr in header_corrections:
                return header_corrections[hdr]
            # remove trailing units
            for regexp in unit_regexps:
                hdr = re.sub(regexp, '', hdr)
            if hdr in header_corrections:
                return header_corrections[hdr]
            return hdr
        file_type_re = re.compile(r'^mm_(.*)_contextual')
        data = {}
        all_headers = set()
        for csv_file in glob(path + '/*.csv'):
            header, rows = csv_to_named_tuple('MMSample', csv_file, mode='rU')
            file_type = file_type_re.match(os.path.basename(csv_file).lower()).groups()[0]
            for row in rows:
                d = dict((get_header_name(t), u) for (t, u) in row._asdict().items())
                d['date_sampled'] = get_date_isoformat(d.get('date_sampled'))
                d['sample_type'] = file_type
                bpa_id = extract_bpa_id(d.pop('bpa_id'))
                all_headers.update(d.keys())
                if bpa_id in data:
                    raise Exception("duplicate contextual metadata: %s" % (bpa_id))
                data[bpa_id] = d
        return all_headers, data

    def get(self, bpa_id):
        # make sure we've got consistent keys (for schema generation)
        template = dict((t, None) for t in self.all_headers)
        if bpa_id in self.data:
            template.update(self.data[bpa_id])
        else:
            logger.warning("no %s metadata available for: %s" % (type(self).__name__, bpa_id))
        return template
