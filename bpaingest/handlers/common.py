from datetime import datetime
import re


METADATA_FILE_NAME = 'metadata.json'
UNSAFE_CHARS = re.compile(r'[^a-zA-Z0-9 !.-]')

TS_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


def shorten(s, length=100):
    if length <= 3:
        return s[:length]
    return s if len(s) <= length else s[:length-3] + '...'


def ts_from_str(s):
    return datetime.strptime(s, TS_FORMAT)


def json_converter(o):
    if isinstance(o, datetime):
        return o.strftime(TS_FORMAT)
