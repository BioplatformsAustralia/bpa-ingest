from bpaingest.libs.common_resources import bsd_md5_re, linux_md5_re


class RawParser:
    def __init__(self, name, match, skip):
        self.skipped = []
        self.no_match = []
        self.matches = []
        self._parse(name, match, skip)

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

    def _parse(self, list_name, match, skip):
        for path in list_name:
            match_path = self._match_path(path)
            if skip is not None and self._matching_regexp(skip, match_path):
                self.skipped.append(path)
                continue
            m = self._matching_regexp(match, match_path)
            if not m:
                self.no_match.append(path)
                continue
            self.matches.append((path, m.groupdict()))
