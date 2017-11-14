# Bioplatforms Australia: CKAN ingest and sync

## Usage

Primary usage information is contained in the comments at the
top of the ```ingest/ingest.sh``` script, which is the gateway
to synchronising the archive.

## Generating CKAN schemas

`bpa-ingest` can generate `ckanext-scheming` schemas.

Usage:

```
$ bpa-ingest -p /tmp/ingest/ makeschema
```

## Tracking metadata

Two types of tracking metadata are stored within this repository.

### Google Drive metadata

The source of truth is "BPA Projects Data Transfer Summary", shared
with BPA in Google Drive. This is maintained by the various project
managers.

To update, use "File", "Download as", "CSV" within Google Sheets 
and replace the CSV sheets in `track-metadata/google-drive`

### BPAM metadata

The source of truth is the BPA Metadata app.

To update, export each of the tracking datasets as CSV using the
export button, then replace the files in `track-metadata/bpam`

 - https://data.bioplatforms.com/bpa/adminsepsis/genomicsmiseqtrack/
 - https://data.bioplatforms.com/bpa/adminsepsis/genomicspacbiotrack/
 - https://data.bioplatforms.com/bpa/adminsepsis/metabolomicslcmstrack/
 - https://data.bioplatforms.com/bpa/adminsepsis/proteomicsms1quantificationtrack/
 - https://data.bioplatforms.com/bpa/adminsepsis/proteomicsswathmstrack/
 - https://data.bioplatforms.com/bpa/adminsepsis/transcriptomicshiseqtrack/

## AWS Lambda

We are gradually adding AWS Lambda functions to this project.

Each Lambda Function will have a `handler()` function which acts as an
entrypoint. These are being collected in `bpaingest/handlers/`

Lambda functions should load their configuration from S3, from a bucket and 
key configured via environment variables. This configuration should be configured
using AWS KMS. The Lambda function can be granted privileges to decrypt the
configuration once it has been read from S3.

To store encrypted data at a key, this pattern works

    $ aws kms encrypt --key-id <key> --plaintext fileb://config.json --output text --query CiphertextBlob | base64 --decode > config.env
    $ aws s3 cp config.enc s3://bucket/key


