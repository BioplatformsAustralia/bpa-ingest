import json
import logging
import re

from bpaingest.handlers.ckan_service import set_up_ckan_service
from bpaingest.handlers.common import GenericHandler, UnrecoverableError


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Handler(GenericHandler):
    ENV_VAR_DEFS = {
        "names": (
            "s3_bucket",
            "s3_config_key",
            "ckan_base_url",
            "ckan_timeout",
            "sns_on_error",
        ),
        "conversions": {"ckan_timeout": float,},
    }
    SNS_ON_ERROR_SUBJECT = "ERROR: Resource Integrity Check"

    def handler(self, event, context):
        resource_id = self._extract_resource_id(event)
        logger.info(resource_id)

        ckan_service = set_up_ckan_service(self.env)
        resource = ckan_service.get_resource_by_id(resource_id)

        computed_etags = [
            v for k, v in resource.items() if re.match(r"^s3etag_\d+$", k)
        ]
        if len(computed_etags) == 0:
            msg = 'The resource "%s" on %s does NOT have any S3 ETags set' % (
                resource_id,
                self.env.ckan_base_url,
            )
            raise UnrecoverableError(msg)

        etag = ckan_service.get_resource_etag(resource["url"])

        if etag not in computed_etags:
            msg = 'The resource "%s" on %s failed the integrity check!' % (
                resource_id,
                self.env.ckan_base_url,
            )
            msg += ' Reported Etag "%s" did NOT match any of the computed Etags %s' % (
                etag,
                computed_etags,
            )
            raise UnrecoverableError(msg)

        ckan_service.mark_resource_passed_integrity_check(resource_id)

    def _extract_resource_id(self, event):
        msg = json.loads(event["Records"][0]["Sns"]["Message"])
        return msg["resource_id"]


handler = Handler(logger)
