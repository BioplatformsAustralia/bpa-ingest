from bpaingest.libs.common_resources import bsd_md5_re, linux_md5_re
from bpaingest.libs.raw_matcher import RawParser
import json


class ManifestParser(RawParser):
    def _parse(self, fname, match, skip):
        with open(fname, 'r', encoding='utf-8') as file:
            json_data = json.load(file)
            for file in json_data['files']:
                match_path = self._match_path(file['name'])
                if skip is not None and self._matching_regexp(skip, match_path):
                    self.skipped.append(file['name'])
                    continue
                m = self._matching_regexp(match, match_path)
                if not m:
                    self.no_match.append(file['name'])
                    continue
                self.matches.append((file['name'], file['md5'], m.groupdict()))


