#!/bin/bash -x

#
# This shell script synchronises the entire BPA legacy archive to CKAN.
#
# Before running:
#   - ensure that the required environment variables are set, including
#     CKAN_USERNAME, CKAN_PASSWORD, CKAN_URL and BPA_*_PASSWORD
#   - if the bpa-ingest tool is installed in a virtualenv, source the
#     activate script
#

apply()
{
  project="$1"
  shift

  # in local dev, set $DEV_MODE and a fixed directory in /tmp will be used to download
  # the metadata. note that you will need to delete `/tmp/ingest` when you need to
  # force a re-download of the metadata

  extra_args1=""
  extra_args2=""
  if [ x"$DEV_MODE" != x ]; then
    extra_args1="-p /tmp/ingest/$project/"
    extra_args2="--skip-resource-checks --metadata-only"
  fi
  bpa-ingest -k "$apikey" -u "$CKAN_URL" $extra_args1 "$action" $extra_args2 "$project" $*
}

bootstrap()
{
  bpa-ingest -k "$apikey" -u "$CKAN_URL" bootstrap
}

base()
{
  apply base-amplicons-control
  apply base-amplicons
  apply base-metagenomics
}

gbr() {
  apply gbr-genomics-amplicons
}

marine_microbes()
{
  apply mm-genomics-amplicons-16s
  apply mm-genomics-amplicons-a16s
  apply mm-genomics-amplicons-18s
  apply mm-genomics-amplicons-16s-control
  apply mm-genomics-amplicons-a16s-control
  apply mm-genomics-amplicons-18s-control
  apply mm-metagenomics
  apply mm-metatranscriptome
}

omg()
{
  apply omg-exoncapture
  apply omg-10xraw
  apply omg-10xprocessed
}

sepsis()
{
  apply sepsis-proteomics-ms1quantification
  apply sepsis-proteomics-analysed
  apply sepsis-proteomics-swathms-combined-sample
  apply sepsis-proteomics-swathms
  apply sepsis-proteomics-swathms-pool
  apply sepsis-genomics-miseq
  apply sepsis-genomics-pacbio
  apply sepsis-metabolomics-lcms
  apply sepsis-transcriptomics-hiseq
  apply sepsis-genomics-analysed
  apply sepsis-metabolomics-analysed
  apply sepsis-transcriptomics-analysed
}

stemcell()
{
  apply stemcells-transcriptome
  apply stemcells-smallrna
  apply stemcells-singlecellrnaseq
  apply stemcells-proteomics
  apply stemcells-metabolomics
  apply stemcells-proteomics-pool
  apply stemcells-proteomics-analysed
  apply stemcells-metabolomics-analysed
}

wheat() {
  apply wheat-cultivars
  apply wheat-pathogens-genomes
}

all()
{
  base
  gbr
  marine_microbes
  omg
  sepsis
  stemcell
  wheat
}


set -e

action="$1"
apikey="$2"
task="$3"

usage()
{
  echo "$0 <action> <apikey>"
  exit 1
}


if [ x"$action" = x ]; then
  usage
fi

if [ x"$apikey" = x ]; then
  usage
fi

if [ x"$task" = x ]; then
  task=all
fi

bpain=$(which bpa-ingest)
if [ x"$bpain" = x ]; then
  echo "Error: bpa-ingest command not found. Aborting."
  exit 1
fi

echo "ingest.sh: running task: $task"
$task
