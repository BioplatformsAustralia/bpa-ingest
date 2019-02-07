#!/bin/bash

#
# This shell script synchronises the entire BPA legacy archive to CKAN.
#
# Before running:
#   - ensure that the required environment variables are set, including
#     CKAN_USERNAME, CKAN_PASSWORD, CKAN_URL and BPA_*_PASSWORD
#   - if the bpa-ingest tool is installed in a virtualenv, source the
#     activate script
#
# Example usage:
#
# in prod, syncing everything (will take some time)
# ./ingest.sh sync <x>
# in prod, syncing all components of the stemcell project (faster)
# ./ingest.sh sync <x> stemcell
# in prod, syncing a single components of the stemcell project (fast)
# ./ingest.sh sync <x> run stemcells-singlecellrnaseq
#
# in local dev, running a single project component:
# DEV_MODE=1 ./ingest.sh sync <x> run stemcells-singlecellrnaseq
#
# to pass additional flags to bpa-ingest, set INGEST_ARGS:
# INGEST_ARGS="--skip-resource-checks" ./ingest.sh sync <x> run stemcells-singlecellrnaseq
#

apply()
{

  project="$1"
  shift

  # in local dev, set $DEV_MODE and a fixed directory in /tmp will be used to download
  # the metadata. note that you will need to delete `/tmp/ingest` when you need to
  # force a re-download of the metadata

  extra_args1=""
  extra_args2="$INGEST_ARGS "

  if [ x"$DEV_MODE" != x ]; then
    extra_args1="-p /tmp/ingest/$project/"
    extra_args2="$INGEST_ARGS --skip-resource-checks --metadata-only --verify-ssl False"
  fi
  echo
  echo ">>> Executing bpa-ingest: $project"
  echo
  bpa-ingest $extra_args1 "$action" -k "$apikey" -u "$CKAN_URL" $extra_args2 "$project" $*
}

bootstrap()
{
    bpa-ingest bootstrap -k "$apikey" -u "$CKAN_URL"
}

base()
{
    apply base-genomics-amplicons $*
    apply base-genomics-amplicons-control $*
    apply base-metagenomics $*
    apply base-site-images $*
}

gbr() {
  apply gbr-genomics-amplicons $*
  apply gbr-genomics-pacbio $*
}

marine_microbes()
{
  apply marine-microbes-genomics $*
  apply marine-microbes-genomics-amplicons-controls $*
  apply marine-microbes-metagenomics $*
  apply marine-microbes-metatranscriptomics $*
}

omg()
{
  # disabled pending resolution of https://github.com/muccg/bpa-archive-ops/issues/329
  # NB: no future data of this type is expected
  # apply omg-10x-raw-agrf
  apply omg-10xprocessed $*
  apply omg-10xraw $*
  apply omg-exoncapture $*
  apply omg-genomics-hiseq $*
  apply omg-genomics-ddrad $*
}

sepsis()
{
  apply sepsis-genomics-analysed $*
  apply sepsis-genomics-miseq $*
  apply sepsis-genomics-pacbio $*
  apply sepsis-metabolomics-analysed $*
  apply sepsis-metabolomics-gcms $*
  apply sepsis-metabolomics-lcms $*
  apply sepsis-proteomics-2dlibrary $*
  apply sepsis-proteomics-analysed $*
  apply sepsis-proteomics-ms1quantification $*
  apply sepsis-proteomics-proteindatabase-analysed $*
  apply sepsis-proteomics-swathms $*
  apply sepsis-proteomics-swathms-combined-sample $*
  apply sepsis-proteomics-swathms-pool $*
  apply sepsis-transcriptomics-analysed $*
  apply sepsis-transcriptomics-hiseq $*
}

stemcell()
{
  apply stemcells-metabolomics $*
  apply stemcells-metabolomics-analysed $*
  apply stemcells-proteomics $*
  apply stemcells-proteomics-analysed $*
  apply stemcells-proteomics-pool $*
  apply stemcells-singlecellrna $*
  apply stemcells-smallrna $*
  apply stemcells-transcriptomics $*
}

wheat() {
  apply wheat-cultivars $*
  apply wheat-pathogens-genomics $*
}

run() {
  apply $*
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

action="$1"
shift
apikey="$1"
shift
task="$1"
shift
taskarg="$1"
shift

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
set -e
"$task" "$taskarg" $*
exit
