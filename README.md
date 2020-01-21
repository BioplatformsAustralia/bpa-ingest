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

    $ aws kms encrypt --key-id <key> --plaintext fileb://config.json --output text --query CiphertextBlob | base64 --decode > config.enc
    $ aws s3 cp config.enc s3://bucket/key


## Local development:
For the development environment, you will need to have your local dev environment for bpa-ckan (consider dockercompose-bpa-ckan to do this).

Before you start, ensure you have installed Python 3.7

Bpa-ingest, atm, is just a python virtualenv (on command line),so to initialise a dev working environment:
```
cd bpa-ingest
git checkout next_release
git pull origin next_release
python3 -m venv ~/.virtual/bpa-ingest
. ~/.virtual/bpa-ingest/bin/activate
pip install -r requirements.txt
python setup.py install
python setup.py develop
```

Then (ensuring that you are still in python virtual env) source the environment variables (including API key), before running the ingest:
```
# if not already in virtual env:
. ~/.virtual/bpa-ingest/bin/activate

# source the environment variables
. /path/to/your/bpa.env

# dump the target state of the data portal to a JSON file for one data type
bpa-ingest -p /tmp/dump-metadata/ dumpstate test.json --dump-re 'omg-genomics-ddrad'

```

Look in /tmp/dump-metadata/ and you will see the working set of metadata sources used by the tool.
Remember to delete the contents of /tmp (or subdirectory you are dumping too), when re-running command:
```
rm -Rf ./tmp/
```