import re


bsd_md5_re = re.compile(r'^MD5 \(([^\)]+)\) = ([0-9a-f]{32})$')
linux_md5_re = re.compile(r'^([0-9a-f]{32}) [\* ](.*)$')


def md5lines(fd):
    "read MD5 lines from `fd` and yield pairs of (md5, path)"
    for line in fd:
        line = line.strip()
        m = bsd_md5_re.match(line)
        if m:
            path, md5 = m.groups()
            yield md5, path
            continue
        m = linux_md5_re.match(line)
        if m:
            md5, path = m.groups()
            yield md5, path
            continue
        raise Exception("Could not parse MD5 line: %s" % line)
