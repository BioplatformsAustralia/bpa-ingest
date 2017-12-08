from datetime import datetime
import re

import requests


METADATA_FILE_NAME = 'metadata.json'
UNSAFE_CHARS = re.compile(r'[^a-zA-Z0-9 !.-]')

TS_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


# Python requests doesn't support setting a timeout by default.
# We're using this Session object to allow specifying a default timeout and
# a way to set the Referrer header (required to pass Django's CSRF checks.
class RequestsSession(requests.Session):
    def __init__(self, *args, default_timeout=None, **kwargs):
        self.default_timeout = default_timeout
        self.referrer = None
        super().__init__(*args, **kwargs)

    def process_response(self, response):
        return response

    def request(self, *args, **kwargs):
        if self.default_timeout:
            kwargs.setdefault('timeout', self.default_timeout)

        if self.referrer is not None:
            headers = kwargs.setdefault('headers', {})
            if 'Referer' not in headers:
                headers['Referer'] = self.referrer

        resp = super().request(*args, **kwargs)
        return self.process_response(resp)


def shorten(s, length=100):
    if length <= 3:
        return s[:length]
    return s if len(s) <= length else s[:length-3] + '...'


def ts_from_str(s):
    return datetime.strptime(s, TS_FORMAT)


def json_converter(o):
    if isinstance(o, datetime):
        return o.strftime(TS_FORMAT)
