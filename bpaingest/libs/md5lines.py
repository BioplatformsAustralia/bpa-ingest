from bpaingest.libs.common_resources import bsd_md5_re, linux_md5_re


class MD5Parser:
    def __init__(self, fname, match, skip):
        self.skipped = []
        self.no_match = []
        self.matches = []
        self._parse(fname, match, skip)

    @classmethod
    def _matching_regexp(cls, regexps, s):
        "return the first matching regular expression from `regexps`"
        matches = [t for t in [regexp.match(s) for regexp in regexps] if t]
        if not matches:
            return None
        return matches[0]

    @classmethod
    def _match_path(cls, s):
        return s.split("/")[-1]

    def _parse(self, fname, match, skip):
        with open(fname) as f:
            for md5, path in md5lines(f):
                match_path = self._match_path(path)
                if skip is not None and self._matching_regexp(skip, match_path):
                    self.skipped.append(path)
                    continue
                m = self._matching_regexp(match, match_path)
                if not m:
                    self.no_match.append(path)
                    continue
                self.matches.append((path, md5, m.groupdict()))


def md5lines(fd):
    "read MD5 lines from `fd` and yield pairs of (md5, path)"
    for line in fd:
        line = line.strip()
        # skip blank lines
        if line == "":
            continue
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
