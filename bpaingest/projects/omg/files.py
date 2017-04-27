from ...libs.md5lines import md5lines
from ...util import make_logger
import re


logger = make_logger(__name__)


tenxtar_filename_re = re.compile("""(?P<basename>.*)\.tar""")


# placeholder until AGRF confirm filename structure
def test_tenxtar_filename_re():
    filenames = [
        'HFMKJBCXY.tar',
        '170314_D00626_0270_BHCGFNBCXY.tar'
    ]
    for filename in filenames:
        assert(tenxtar_filename_re.match(filename) is not None)


def parse_md5_file(md5_file, regexps):
    with open(md5_file) as f:
        for md5, path in md5lines(f):
            # skip AGRF checksum program
            if path == 'TestFiles.exe':
                continue
            matches = filter(None, (regexp.match(path.split('/')[-1]) for regexp in regexps))
            m = None
            if matches:
                m = matches[0]
            if m:
                yield path, md5, m.groupdict()
            else:
                yield path, md5, None
