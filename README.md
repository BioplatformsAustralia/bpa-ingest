# Bioplatforms Australia: CKAN ingest and sync

## Usage

Primary usage information is contained in the comments at the
top of the ```ingest/ingest.sh``` script, which is the gateway
to synchronising the archive.

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

