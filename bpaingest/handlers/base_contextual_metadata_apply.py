import json
import logging

import boto3

from bpaingest.handlers.ckan_service import set_up_ckan_service
from bpaingest.handlers.common import GenericHandler, shorten
from bpaingest.libs.ingest_utils import get_clean_number


logger = logging.getLogger()
logger.setLevel(logging.INFO)


sns = boto3.client('sns')


class Handler(GenericHandler):
    '''Applies BASE contextual metadata values to packages with a given BPA ID.

    The function should be set up to be triggered by SNS messages that have the bpa_id and values
    to apply in them.
    The packages matching the BPA ID will be looked up from CKAN and an SNS message will be created
    for each package, containing the package id and the values that have to be applied.
    The SNS message will be created only if the package doesn't have the values already applied.
    Also only the values that aren't already set on the package will be in the outgoing SNS message.
    The sns_ckan_patch_package should be set to the SNS topic arn that will receive the outgoing messages.
    '''

    ENV_VAR_DEFS = {
        'names': ('s3_bucket', 's3_config_key', 'ckan_base_url', 'ckan_timeout',
            'sns_ckan_patch_package',
            'sns_on_success', 'sns_on_error'),
        'optional': ('sns_on_success', ),
        'conversions': {
            'ckan_timeout': float,
        },
    }
    SNS_ON_ERROR_SUBJECT = 'ERROR: BASE Contextual Metadata Apply'

    def handler(self, event, context):
        bpa_id, metadata = self._extract_data(event)
        logger.info('Processing BPA ID %s', bpa_id)

        ckan_service = set_up_ckan_service(self.env)

        packages = ckan_service.get_packages_by_bpa_id(bpa_id)
        pids_and_changes = [(p['id'], changes(p, metadata)) for p in packages]
        packages_with_changes = [x for x in pids_and_changes if len(x[1]) > 0]
        for pid, updates in packages_with_changes:
            self.sns_ckan_patch_package(pid, updates)

        unchanged_package_ids = [x[0] for x in pids_and_changes if len(x[1]) == 0]
        self.sns_success(bpa_id, packages_with_changes, unchanged_package_ids)

    def sns_success(self, bpa_id, packages_with_changes, unchanged_package_ids):
        subject = shorten('BASE Apply Contextual Metadata - BPA ID %s' % bpa_id)
        changed_count = len(packages_with_changes)
        unchanged_count = len(unchanged_package_ids)
        msg = 'Processed BPA ID %s, found %d packages, %d already up-to-date, sent SNS patch requests for %d.' % (
            bpa_id, changed_count + unchanged_count, unchanged_count, changed_count)

        logger.info(msg)
        if not self.env.sns_on_success:
            return
        sns.publish(TopicArn=self.env.sns_on_success,
            Subject=subject,
            Message=msg)

    def sns_ckan_patch_package(self, package_id, updates):
        default = 'Patch CKAN package %s' % package_id
        json_data = json.dumps({
            'package_id': package_id,
            'updates': updates,
        })
        data = {
            'default': default,
            'lambda': json_data,
            'email-json': json_data,
        }

        sns.publish(TopicArn=self.env.sns_ckan_patch_package,
            MessageStructure='json',
            Message=json.dumps(data))

    def _extract_data(self, event):
        message = json.loads(event['Records'][0]['Sns']['Message'])
        return (message['bpa_id'], message['metadata'])


handler = Handler(logger)


def changes(d1, d2):
    def floats_close_enough(f1, f2, epsilon=1e-10):
        return abs(f1 - f2) < epsilon

    def equals(v1, v2):
        if isinstance(v1, float) or isinstance(v2, float):
            f1 = v1 if isinstance(v1, float) else get_clean_number(v1)
            f2 = v2 if isinstance(v2, float) else get_clean_number(v2)
            return floats_close_enough(f1, f2)
        return v1 == v2

    def has_value(d, k, v):
        if v is None:
            return d.get(k) is None
        if k not in d:
            return False
        return equals(d[k], v)

    return {k: v for k, v in d2.items() if not has_value(d1, k, v)}
