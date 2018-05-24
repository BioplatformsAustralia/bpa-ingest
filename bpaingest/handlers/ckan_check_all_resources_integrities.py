from itertools import filterfalse
import json
import logging

import boto3
from datetime import datetime, timedelta

from bpaingest.handlers.common import GenericHandler, ts_from_iso_str
from bpaingest.handlers.ckan_service import set_up_ckan_service


logger = logging.getLogger()
logger.setLevel(logging.INFO)


sns = boto3.client('sns')


class Handler(GenericHandler):
    ENV_VAR_DEFS = {
        'names': (
            's3_bucket', 's3_config_key',
            'ckan_base_url', 'ckan_timeout',
            'integrity_check_expiry_days',
            'sns_check_resource_integrity',
            'sns_on_error'),
        'conversions': {
            'ckan_timeout': float,
            'integrity_check_expiry_days': int,
        }
    }
    SNS_ON_ERROR_SUBJECT = 'ERROR: Resource Integrity Check'

    def handler(self, event, context):
        do_force = event is not None and event.get('force', False)

        ckan_service = set_up_ckan_service(self.env)
        resources = ckan_service.get_all_resources()

        def has_valid_verification(resource):
            if 's3_etag_verified_at' not in resource:
                return False
            verified_at = ts_from_iso_str(resource.get('s3_etag_verified_at'))
            expiration_delta = timedelta(days=self.env.integrity_check_expiry_days)
            return (datetime.utcnow() - expiration_delta) < verified_at

        if not do_force:
            resources = filterfalse(has_valid_verification, resources)

        for resource in resources:
            logger.info('Processing resource %s', resource['id'])
            self.sns_check_resource_integrity(resource['id'])

    def sns_check_resource_integrity(self, resource_id):
        data = {
            'default': resource_id,
            'lambda': json.dumps({'resource_id': resource_id})
        }

        sns.publish(
            TopicArn=self.env.sns_check_resource_integrity,
            MessageStructure='json',
            Message=json.dumps(data))


handler = Handler(logger)
