#!/usr/bin/env python

import sys
import re
from bpaingest.libs.md5lines import md5lines
from bpaingest.util import make_logger

logger = make_logger(__name__)

# 27146_AHYFU_R1.fastq.gz
external_re = re.compile("""
    (?P<id>\d{4,6})_
    (?P<flow>\w{5})_
    (?P<read>[R|I][1|2])\.fastq\.gz
""", re.VERBOSE)


def main():
    with open(sys.argv[1]) as fd:
        for checksum, path in md5lines(fd):
            amplicon, _, fname = path.split('/')
            m = external_re.match(fname)
            if not m:
                logger.error('cannot match: %s' % path)
                continue
            obj = m.groupdict()
            obj.update({
                'extraction': '1',
                'amplicon': amplicon,
                'vendor': 'UNSW',
                'index': 'UNKNOWN',
                'runsamplenum': 'UNKNOWN',
                'lane': 'L001',
            })
            target = '{id}_{extraction}_{amplicon}_{vendor}_{index}_{flow}_{runsamplenum}_{lane}_{read}'.format(**obj)
            print([fname, target])


if __name__ == '__main__':
    main()
