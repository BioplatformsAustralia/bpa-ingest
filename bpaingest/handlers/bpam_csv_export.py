from collections import namedtuple
from functools import partial
import hashlib
import json
import os
import re
import traceback
from urllib.parse import urlparse, urlunparse

from bs4 import BeautifulSoup

import boto3
from datetime import datetime

from bpaingest.handlers.common import (
    METADATA_FILE_NAME, UNSAFE_CHARS, TS_FORMAT,
    RequestsSession,
    shorten, ts_from_str, json_converter)


s3 = boto3.client('s3')
sns = boto3.client('sns')
kms = boto3.client('kms')


class AdminService:
    def __init__(self, session, credentials, project_url):
        self.session = session
        self.credentials = credentials
        self.session.process_response = self.process_response
        self.project_url = project_url

    def process_response(self, resp):
        self.session.referrer = resp.request.url
        if self.was_redirected_to_login(resp):
            resp = self.login(resp)
        return resp

    def get_login_form(self, resp):
        soup = BeautifulSoup(resp.text, 'html.parser')
        return soup.find('form', id='login-form')

    def was_redirected_to_login(self, resp):
        if len(resp.history) == 0 or resp.history[0].status_code != 302:
            return False
        return self.get_login_form(resp) is not None

    def admin_url(self, path):
        scheme, netloc = urlparse(self.project_url)[:2]
        return urlunparse([scheme, netloc, path] + [''] * 3)

    def hidden_fields(self, form):
        return {f['name']: f['value'] for f in form.findAll('input', type='hidden')}

    def login(self, resp):
        form = self.get_login_form(resp)
        payload = self.credentials.copy()
        payload.update(self.hidden_fields(form))

        login_resp = self.session.post(self.admin_url(form['action']), data=payload)
        if login_resp.status_code != 200:
            raise Exception('Could not log in to %s' % self.admin_url(form['action']))
        return login_resp

    def get_models(self):
        resp = self.session.get(self.project_url)

        soup = BeautifulSoup(resp.text, 'html.parser')
        rows = soup.select('#content-main table tr')

        def name(row):
            return row.find('th').text

        def change_url(row):
            return self.admin_url(row.find('a', text='Change').get('href'))

        def file_name(row):
            return re.sub(UNSAFE_CHARS, '_', name(row)) + '.csv'

        return [(name(r), change_url(r), file_name(r)) for r in rows]

    def get_model_data(self, url, fmt='csv'):
        export_url = os.path.join(url, 'export/')
        resp = self.session.get(export_url)

        soup = BeautifulSoup(resp.text, 'html.parser')
        form = soup.find('form')

        format_select = form.find('select', id='id_file_format')

        def find_format_value(fmt):
            for option in format_select.select('option'):
                if option.text.strip().lower() == fmt.lower():
                    return option['value']

        payload = self.hidden_fields(form)
        payload[format_select['name']] = find_format_value(fmt)

        resp = self.session.post(export_url, data=payload)
        return resp.text


def set_up_credentials(env):
    config = s3.get_object(Bucket=env.s3_bucket, Key=env.s3_config_key)
    credentials = json.loads(kms.decrypt(CiphertextBlob=config['Body'].read())['Plaintext'])
    return credentials


def set_up_admin_service(env):
    credentials = set_up_credentials(env)
    session = RequestsSession(default_timeout=env.bpam_admin_timeout)
    return AdminService(session, credentials=credentials, project_url=env.bpam_project_url)


Model = namedtuple('Model', ('name', 'url', 'file_name', 'last_modified_at', 'md5'))


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

    def add_model(self, model):
        models = self.data.setdefault('models', [])
        models.append(model)

    def get_model(self, model_name):
        for m in (Model(*m) for m in self.data.get('models', [])):
            if m.name == model_name:
                return m

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
    names = (
        'bpam_project_url', 's3_bucket', 's3_output_prefix', 's3_config_key', 'bpam_admin_timeout',
        'sns_on_success', 'sns_on_change', 'sns_on_error')
    optional = set(('sns_on_success', 'sns_on_change', 'sns_on_error'))

    conversions = {
        'bpam_admin_timeout': float
    }
    EnvVars = namedtuple('EnvVars', names)

    def env_val(name):
        conversion = conversions.get(name, lambda x: x)
        return conversion(os.environ[name.upper()] if name not in optional else os.environ.get(name.upper()))

    return EnvVars(*[env_val(name) for name in names])


def handler(event, context):
    env = None
    try:
        env = get_env_vars()
        return _handler(env, event, context)
    except Exception as exc:
        if env:
            sns_on_error(env, exc)
        raise


def _handler(env, event, context):
    meta = Metadata(env.s3_bucket, env.s3_output_prefix)
    meta.load()

    sns_success = partial(sns_result, env.sns_on_success, env.bpam_project_url)
    sns_change = partial(sns_result, env.sns_on_change, env.bpam_project_url)

    admin_service = set_up_admin_service(env)
    models = admin_service.get_models()

    cur_meta = Metadata(env.s3_bucket, env.s3_output_prefix)
    cur_meta.last_processed_at = datetime.now()

    def export_csv(file_name, data, md5, create=False):
        key_name = os.path.join(env.s3_output_prefix, file_name)
        action = 'Creating' if create else 'Updating'
        print('%s %s in bucket %s' % (action, key_name, env.s3_bucket), md5)
        s3.put_object(Bucket=env.s3_bucket, Key=key_name, Body=data)

    changed_models = []
    for name, url, file_name in models:
        data = admin_service.get_model_data(url)
        md5 = hashlib.md5(data.encode('utf-8')).hexdigest()

        previous_model = meta.get_model(name)

        if previous_model is not None and previous_model.md5 == md5:
            # Data in csv hasn't changed
            cur_meta.add_model(previous_model)
        else:
            export_csv(file_name, data, md5, create=previous_model is None)
            model = Model(name, url, file_name, cur_meta.last_processed_at, md5)
            cur_meta.add_model(model)
            changed_models.append(model)

    cur_meta.data['last_run_changed_files_count'] = len(changed_models)
    cur_meta.save()

    msg = 'Changed %d files from a total of %d' % (len(changed_models), len(models))
    print(msg)
    sns_success(msg, changed_models)
    if len(changed_models) > 0:
        sns_change(msg, changed_models)
    return len(changed_models)


def sns_result(topic_arn, project_url, msg, changed_models=()):
    if topic_arn is None:
        return

    subject = shorten(
        '%s - %s' % ('Changes in' if len(changed_models) > 0 else 'No changes in', project_url))

    data = {
        'default': msg,
        'email-json': json.dumps({
            'msg': msg,
            'changed_models': [dict(s._asdict().items()) for s in changed_models],
        }, default=json_converter)
    }

    sns.publish(
        TopicArn=topic_arn,
        Subject=subject,
        MessageStructure='json',
        Message=json.dumps(data))


def sns_on_error(env, exc):
    if env is None or getattr(env, 'sns_on_error') is None:
        return
    subject = shorten('ERROR in - %s' % (getattr(env, 'bpam_project_url', 'Unknown')))
    msg = '\n'.join((str(exc), traceback.format_exc()))
    sns.publish(TopicArn=env.sns_on_error, Subject=subject, Message=msg)
