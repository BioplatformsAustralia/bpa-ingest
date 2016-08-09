# -*- coding: utf-8 -*-


class MD5ParsedLine(object):
    """
    A single line parsed from the md5 file
    """
    def __init__(self, pattern, line):
        self.pattern = pattern
        self._line = line
        self._ok = False
        self.__parse_line()
        self.md5 = None
        self.filename = None
        self.__parse_line()

    def is_ok(self):
        return self._ok

    def __parse_line(self):
        """
        Unpack the md5 line into:
        md5, filename, and md5data dict based on the regular expression pattern
        """
        self.md5, self.filename = self._line.split()
        matched = self.pattern.match(self.filename)
        if matched:
            self.md5data = matched.groupdict()
            self._ok = True

    def __str__(self):
        return '{} {}'.format(self.filename, self.md5)


def parse_md5_file(pattern, md5_file):
    """
    Parse md5 file
    Returns:
       list of MD5ParsedLine objects
    """
    data = []
    with open(md5_file) as f:
        for line in f.read().splitlines():
            line = line.strip()
            if line == '':
                continue

            parsed_line = MD5ParsedLine(pattern, line)
            if parsed_line.is_ok():
                data.append(parsed_line)
    return data
