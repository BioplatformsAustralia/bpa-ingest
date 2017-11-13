from collections import namedtuple
import csv
import hashlib
from httplib2 import Http
import io
import json
import os
import re
from base64 import b64decode

import boto3
from datetime import datetime

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


# Set timeout to prevent paying if we can't connect to the Google APIs
METADATA_FILE_NAME = 'metadata.json'
UNSAFE_CHARS = re.compile(r'[^a-zA-Z0-9 !.-]')

TS_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

s3 = boto3.client('s3')
kms = boto3.client('kms')


def set_up_credentials(env):
    config = s3.get_object(Bucket=env.s3_bucket, Key=env.s3_config_key)
    json_data = json.loads(kms.decrypt(CiphertextBlob=config['Body'].read())['Plaintext'])
    scopes = ['https://www.googleapis.com/auth/drive.readonly']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(json_data, scopes=scopes)
    return credentials.authorize(Http(timeout=env.google_api_timeout))


Sheet = namedtuple('Sheet', ('name', 'file_name', 'last_modified_at', 'md5'))


class Metadata:
    def __init__(self, bucket_name, root_dir):
        self.bucket_name = bucket_name
        self.root_dir = root_dir
        self.data = {}

    @property
    def _s3_key_name(self):
        return os.path.join(self.root_dir, METADATA_FILE_NAME)

    @property
    def last_processed_at(self):
        s = self.data.get('last_processed_at')
        return ts_from_str(s) if s else None

    @last_processed_at.setter
    def last_processed_at(self, dt):
        self.data['last_processed_at'] = dt.strftime(TS_FORMAT)

    @property
    def file_last_modified_at(self):
        s = self.data.get('file_last_modified_at')
        return ts_from_str(s) if s else None

    @file_last_modified_at.setter
    def file_last_modified_at(self, dt):
        self.data['file_last_modified_at'] = dt.strftime(TS_FORMAT)

    def add_sheet(self, sheet):
        sheets = self.data.setdefault('sheets', [])
        sheets.append(sheet)

    def get_sheet(self, sheet_name):
        for s in (Sheet(*s) for s in self.data.get('sheets', [])):
            if s.name == sheet_name:
                return s

    def load(self):
        try:
            obj = s3.get_object(Bucket=self.bucket_name, Key=self._s3_key_name)
            self.data = json.loads(obj['Body'].read())
        except s3.exceptions.NoSuchBucket:
            raise ValueError('Bucket "%s" does not exist' % self.bucket_name)
        except s3.exceptions.NoSuchKey:
            pass

    def save(self):
        if self.last_processed_at is None:
            self.last_processed_at = datetime.now()
        try:
            s3.put_object(Bucket=self.bucket_name, Key=self._s3_key_name, Body=json.dumps(self.data, default=json_converter))
        except s3.exceptions.NoSuchBucket:
            raise ValueError('Bucket "%s" does not exist' % self.bucket_name)


def get_env_vars():
    names = ('file_id', 's3_bucket', 's3_output_prefix', 's3_config_key', 'google_api_timeout')
    EnvVars = namedtuple('EnvVars', names)

    def env_val(name):
        return os.environ[name.upper()]

    return EnvVars(*[env_val(name) for name in names])


def handler(event, context):
    env = get_env_vars()

    http_auth = set_up_credentials(env)

    meta = Metadata(env.s3_bucket, env.s3_output_prefix)
    meta.load()

    drive_service = build('drive', 'v3', http=http_auth)

    file_info = drive_service.files().get(fileId=env.file_id, fields='id, modifiedTime').execute()
    file_last_modified_at = ts_from_str(file_info['modifiedTime'])

    if meta.last_processed_at is not None and meta.last_processed_at >= file_last_modified_at:
        print('Latest version of file already processed. Nothing to do...')
        return 0

    sheets_service = build('sheets', 'v4', http=http_auth)

    response = sheets_service.spreadsheets().get(spreadsheetId=env.file_id).execute()

    sheets = [s['properties']['title'] for s in response['sheets']]
    file_names = [re.sub(UNSAFE_CHARS, '_', name) + '.csv' for name in sheets]

    cur_meta = Metadata(env.s3_bucket, env.s3_dir)
    cur_meta.file_last_modified_at = file_last_modified_at

    def export_csv(file_name, data, md5, create=False):
        key_name = os.path.join(env.s3_dir, file_name)
        action = 'Creating' if create else 'Updating'
        print('%s %s in bucket %s' % (action, key_name, env.s3_bucket), md5)
        s3.put_object(Bucket=env.s3_bucket, Key=key_name, Body=data)

    changed = 0
    for sheet_title, file_name in zip(sheets, file_names):
        data = get_spreadsheet_data(sheets_service, env.file_id, sheet_title)
        md5 = hashlib.md5(data.encode('utf-8')).hexdigest()

        previous_sheet = meta.get_sheet(sheet_title)

        if previous_sheet is not None and previous_sheet.md5 == md5:
            # Data is sheet hasn't changed
            cur_meta.add_sheet(previous_sheet)
        else:
            export_csv(file_name, data, md5, create=previous_sheet is None)
            cur_meta.add_sheet(Sheet(sheet_title, file_name, cur_meta.file_last_modified_at, md5))
            changed += 1

    cur_meta.data['last_run_changed_files_count'] = changed
    cur_meta.save()

    print('Changed %d files from a total of %d' % (changed, len(sheets)))
    return changed


def get_spreadsheet_data(service, file_id, title):
    response = service.spreadsheets().values().batchGet(spreadsheetId=file_id, ranges=title).execute()
    values = response.get('valueRanges')[0].get('values')

    f = io.StringIO()
    writer = csv.writer(f)
    for row in values:
        writer.writerow(row)

    return f.getvalue()


def ts_from_str(s):
    return datetime.strptime(s, TS_FORMAT)


def json_converter(o):
    if isinstance(o, datetime):
        return o.strftime(TS_FORMAT)
