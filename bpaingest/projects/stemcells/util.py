from ...util import make_logger

logger = make_logger(__name__)


# these errors are in a large number of submission sheets,
# so it is more practical to correct here in code
PLATFORM_CORRECTIONS = {
    'LCMS': 'LC/MS',
    'GCMS': 'GC/MS',
    'GC/MS media': 'GC/MS',
    'LC-MS//MS': 'LC/MS',
    'LC-MS': 'LC/MS',
    'GC-MS': 'GC/MS'
}

VALID_PLATFORMS = (
    'LC/MS',
    'GC/MS',
    'Transcriptome',
    'Small RNA',
    'Single Cell RNAseq',
    '2D LC-MS/MS -1D LC-MS/MS',
    '1D LC-MS/MS',
    '1D LC-MS/SWATH',
    'DIA mass spec',
    'MS1 quantification',
    'DIA quantification',
    'Negative Ion (25 - 1200 mz; 0.9 spectra')


def fix_analytical_platform(s):
    if not s:
        return s
    s = PLATFORM_CORRECTIONS.get(s, s)
    if s == 'LCMS':
        return 'LC-MS'
    if s == 'GCMS':
        return 'GC-MS'
    if s in VALID_PLATFORMS:
        return s
    logger.warning('Unknown analytical platform: %s' % (s))
    return s
