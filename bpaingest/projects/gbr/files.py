import re

from ...util import make_logger
from ...libs.md5lines import md5lines

logger = make_logger(__name__)

AMPLICON_FILE_PATTERN = """
    (?P<bpa_id>\d{4,6})_
    GBR_
    (?P<vendor>AGRF|UNSW)_
    (?P<amplicon>16S|18S|A16S|ITS)_
    (?P<reach>R\d{3,4}-\d{3,4})_
    (?P<flow_id>\w{5})_
    (?P<index>[GATC]{8}_[GATC]{8})_
    (?P<post>.*)
"""
amplicon_filename_re = re.compile(AMPLICON_FILE_PATTERN, re.VERBOSE)


PACBIO_FILE_PATTERN = """
    ^(?P<bpa_id>\d{4,6})_
    GBR_
    (?P<vendor>AGRF|UNSW)_
    (?P<run_number>m\d+_\d+_\d+)_
    (?P<flow_cell_id>c\d+)_
    .*
"""
pacbio_filename_re = re.compile(PACBIO_FILE_PATTERN, re.VERBOSE)


def _file_from_line(line):
    obj = {
        'filename': line.filename,
        'name': line.filename,
        'md5': line.md5,
        'amplicon': line.md5data['amplicon'],
        'reach': line.md5data['reach'],
        'flow_id': line.md5data['flow_id'],
        'index': line.md5data['index']
    }
    return line.md5data['id'], obj


def _get_parsed_lines(path):
    """
    Return list of parsed md5parsedline objects
    """
    def is_md5(path):
        if path.isfile() and path.ext == '.md5':
            return True

    logger.info('Ingesting GBR Amplicon File data from md5 files found in {0}'.format(path))
    md5parsedlines = []
    for md5_file in path.walk(filter=is_md5):
        logger.info('Processing GBR md5 checksum file {0}'.format(md5_file))
        with open(md5_file) as md5_fd:
            for md5, path in md5lines(md5_fd):
                m = re.match(AMPLICON_FILE_PATTERN, path)
                if m:
                    md5parsedlines.append(m.groupdict())
    return md5parsedlines


def files_from_md5(path):
    files = []
    lines = _get_parsed_lines(path)
    for line in lines:
        files.append(_file_from_line(line))
    return files
