import json
import logging
import os
import tempfile

import boto3

from bpaingest.handlers.common import GenericHandler, shorten
from bpaingest.projects.base.contextual import BASESampleContextual


logger = logging.getLogger()
logger.setLevel(logging.INFO)


sns = boto3.client("sns")


class Handler(GenericHandler):
    """Processes each row of a BASE contextual metadata spreadsheet file uploaded to S3.

    The function should be set up to be triggered by ObjectCreate S3 events for a bucket and path
    where contextual metadata spreadsheets will be uploaded.
    The function will read each row and will create one SNS message for each, containing the sample_id
    and the metadata (rest of the values in the spreadsheet for the row).
    The sns_apply_to_sample_id should be set to the SNS topic arn that will receive these messages.
    """

    ENV_VAR_DEFS = {
        "names": ("sns_apply_to_sample_id", "sns_on_success", "sns_on_error"),
    }
    SNS_ON_ERROR_SUBJECT = "ERROR: BASE Contextual Metadata Sheet"

    def handler(self, event, context):
        bucket, key = self._extract_s3_key_name(event)
        self.metadata_s3_key = "s3://" + os.path.join(bucket, key)
        fname = os.path.basename(key.rstrip("/"))

        s3 = boto3.resource("s3")

        with tempfile.TemporaryDirectory() as dirname:
            bucket = s3.Bucket(bucket)
            full_fname = os.path.join(dirname, fname)
            bucket.download_file(key, full_fname)

            contextual = BASESampleContextual(dirname)
            rows = list(contextual.sample_metadata.items())
            for sample_id, values in rows:
                self.sns_publish_apply_metadata(sample_id, values)
        self.sns_success(rows)

    def sns_success(self, rows):
        subject = shorten("BASE Contextual Metadata - %s" % self.metadata_s3_key)
        msg = (
            "Processed %s, sent SNS messages to apply contextual metadata to %d BPA Ids."
            % (self.metadata_s3_key, len(rows))
        )

        sns.publish(TopicArn=self.env.sns_on_success, Subject=subject, Message=msg)

    def sns_publish_apply_metadata(self, sample_id, metadata):
        default = "Apply contextual metadata to sample sample_id:%s from %s" % (
            sample_id,
            self.metadata_s3_key,
        )
        json_data = json.dumps({"sample_id": sample_id, "metadata": metadata,})
        data = {
            "default": default,
            "lambda": json_data,
            "email-json": json_data,
        }

        sns.publish(
            TopicArn=self.env.sns_apply_to_sample_id,
            MessageStructure="json",
            Message=json.dumps(data),
        )

    def _extract_s3_key_name(self, event):
        return (
            event["Records"][0]["s3"]["bucket"]["name"],
            event["Records"][0]["s3"]["object"]["key"],
        )


handler = Handler(logger)
