from collections import namedtuple
from datetime import datetime
import json
import logging
import os
import re
import traceback

import boto3
import requests


METADATA_FILE_NAME = "metadata.json"
UNSAFE_CHARS = re.compile(r"[^a-zA-Z0-9 !.-]")

TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
ISO_TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"

s3 = boto3.client("s3")
sns = boto3.client("sns")
kms = boto3.client("kms")


# Async calls of Lambdas will be auto-retried twice with a timeout before giving up
# Raise UnrecoverableError when you don't want to autoretry. Note: that these errors will be handled
# and NOT re-raised by the lambda (ie. the lambda won't be reported as having failed)
class UnrecoverableError(Exception):
    pass


def set_up_credentials(env):
    config = s3.get_object(Bucket=env.s3_bucket, Key=env.s3_config_key)
    credentials = json.loads(
        kms.decrypt(CiphertextBlob=config["Body"].read())["Plaintext"]
    )
    return credentials


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
            kwargs.setdefault("timeout", self.default_timeout)

        if self.referrer is not None:
            headers = kwargs.setdefault("headers", {})
            if "Referer" not in headers:
                headers["Referer"] = self.referrer

        resp = super().request(*args, **kwargs)
        return self.process_response(resp)


class GenericHandler:
    SNS_ON_ERROR_SUBJECT = "ERROR"

    def __init__(self, logger=None):
        self.env = None
        self.logger = logger or logging.getLogger()

    def get_env_vars(self):
        names = self.ENV_VAR_DEFS["names"]
        optional = self.ENV_VAR_DEFS.get("optional", ())
        conversions = self.ENV_VAR_DEFS.get("conversions", {})

        def env_val(name):
            conversion = conversions.get(name, lambda x: x)
            return conversion(
                os.environ[name.upper()]
                if name not in optional
                else os.environ.get(name.upper())
            )

        EnvVars = namedtuple("EnvVars", names)
        return EnvVars(*[env_val(name) for name in names])

    def __call__(self, event, context):
        try:
            self.env = self.get_env_vars()
            return self.handler(event, context)
        except UnrecoverableError as exc:
            self.logger.exception(exc)
            self.sns_on_error(exc)
        except Exception as exc:
            self.logger.exception(exc)
            self.sns_on_error(exc)
            raise

    def sns_on_error(self, exc):
        if self.env is None or getattr(self.env, "sns_on_error") is None:
            return
        msg = "\n".join((str(exc), traceback.format_exc()))
        sns.publish(
            TopicArn=self.env.sns_on_error,
            Subject=self.SNS_ON_ERROR_SUBJECT,
            Message=msg,
        )


def shorten(s, length=100):
    if length <= 3:
        return s[:length]
    return s if len(s) <= length else s[: length - 3] + "..."


def ts_from_str(s):
    return datetime.strptime(s, TS_FORMAT)


def ts_from_iso_str(s):
    return datetime.strptime(s, ISO_TS_FORMAT)


def json_converter(o):
    if isinstance(o, datetime):
        return o.strftime(TS_FORMAT)


# CKAN supports iso dates, AWS services TS_FORMAT
def json_ckan_converter(o):
    if isinstance(o, datetime):
        return datetime.isoformat(o)
