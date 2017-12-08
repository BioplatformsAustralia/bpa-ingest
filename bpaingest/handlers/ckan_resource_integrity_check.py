from collections import namedtuple
import json
import os
import re
import traceback

import boto3
from datetime import datetime

from bpaingest.handlers.common import RequestsSession


s3 = boto3.client('s3')
sns = boto3.client('sns')
kms = boto3.client('kms')


class CKANService:
    def __init__(self, session, credentials, base_url):
        self.session = session
        self.credentials = credentials
        self.base_url = base_url
        self.resource_url = os.path.join(self.base_url, 'api/3/action/resource_show')

    def get_resource_by_id(self, resource_id):
        resp = self.session.get(self.resource_url, params={'id': resource_id})
        try:
            resp.raise_for_status()
            json_resp = resp.json()
            if not json_resp['success']:
                raise Exception('Resource show returned success False')
            return json_resp['result']
        except Exception as exc:
            msg = 'Resource show (%s) for resource "%s" was NOT successful! ' % (
                resp.request.url, resource_id)
            print(msg)
            raise Exception(msg) from exc

    def get_resource_etag(self, url):
        resp = self.session.head(url)
        resp.raise_for_status()
        etag = resp.headers.get('ETag', '').strip('"')
        if not etag:
            raise Exception('ETag header missing for URL %s' % url)
        return etag


def set_up_credentials(env):
    config = s3.get_object(Bucket=env.s3_bucket, Key=env.s3_config_key)
    credentials = json.loads(kms.decrypt(CiphertextBlob=config['Body'].read())['Plaintext'])
    return credentials


def set_up_ckan_service(env):
    credentials = set_up_credentials(env)
    session = RequestsSession(default_timeout=env.ckan_timeout)
    return CKANService(session, credentials=credentials, base_url=env.ckan_base_url)


def get_env_vars():
    names = ('s3_bucket', 's3_config_key', 'ckan_base_url', 'ckan_timeout', 'sns_on_error')

    conversions = {
        'ckan_timeout': float
    }
    EnvVars = namedtuple('EnvVars', names)

    def env_val(name):
        conversion = conversions.get(name, lambda x: x)
        return conversion(os.environ[name.upper()])

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
    resource_id = event['resource_id']

    ckan_service = set_up_ckan_service(env)
    resource = ckan_service.get_resource_by_id(resource_id)

    computed_etags = [v for k, v in resource.items() if re.match(r'^s3etag_\d+$', k)]
    if len(computed_etags) == 0:
        msg = 'The resource "%s" on %s does NOT have any S3 ETags set' % (resource_id, env.ckan_base_url)
        raise Exception(msg)

    etag = ckan_service.get_resource_etag(resource['url'])

    if etag not in computed_etags:
        msg = 'The resource "%s" on %s failed the integrity check!' % (resource_id, env.ckan_base_url)
        msg += ' Reported Etag "%s" did NOT match any of the computed Etags %s' % (etag, computed_etags)
        raise Exception(msg)

    # TODO
    # patch CKAN resource with s3_etag_verified_at: NOW


def sns_on_error(env, exc):
    if env is None or getattr(env, 'sns_on_error') is None:
        return
    subject = 'ERROR: CKAN resource integrity check'
    msg = '\n'.join((str(exc), traceback.format_exc()))
    sns.publish(TopicArn=env.sns_on_error, Subject=subject, Message=msg)
