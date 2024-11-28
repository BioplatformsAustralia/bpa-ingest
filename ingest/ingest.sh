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

export GDAL_DATA=${VIRTUAL_ENV}/lib/python3.7/site-packages/fiona/gdal_data/

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

ausarg() {
  apply ausarg-illumina-fastq $*
  apply ausarg-pacbio-hifi $*
  apply ausarg-ont-promethion $*
  apply ausarg-exoncapture $*
  apply ausarg-hi-c $*
  apply ausarg-genomics-dart $*
  apply ausarg-genomics-ddrad $*
}

marine_microbes()
{
  apply marine-microbes-genomics-amplicons $*
  apply marine-microbes-genomics-amplicons-control $*
  apply marine-microbes-metagenomics $*
  apply marine-microbes-metatranscriptomics $*
}

omg()
{
  apply omg-10x-raw-agrf $*
  apply omg-10xprocessed $*
  apply omg-10xraw $*
  apply omg-exoncapture $*
  apply omg-genomics-hiseq $*
  apply omg-genomics-ddrad $*
  apply omg-transcriptomics-nextseq $*
  apply omg-novaseq $*
  apply omg-novaseq-whole-genome $*
  apply omg-ont-promethion $*
  apply omg-analysed-data $*
  apply omg-pacbio $*
  apply omg-pacbio-genome-assembly $*
  apply omg-genomics-dart $*
}

tsi()
{
  apply tsi-pacbio-hifi $*
  apply tsi-genomics-ddrad $*
  apply tsi-illumina-shortread $*
  apply tsi-illumina-fastq $*
  apply tsi-genome-assembly $*
  apply tsi-hi-c $*
  apply tsi-genomics-dart $*
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
  apply stemcells-transcriptome-analysed $*
}

wheat() {
  apply wheat-cultivars $*
  apply wheat-pathogens-genomics $*
}

gap() {
  apply gap-illumina-shortread $*
  apply gap-ont-minion $*
  apply gap-ont-promethion $*
  apply gap-genomics-10x $*
  apply gap-hi-c $*
  apply gap-genomics-ddrad $*
  apply gap-pacbio-hifi $*
}

amd() {
  apply amd-genomics-amplicons $*
  apply amd-genomics-amplicons-control $*
  apply amd-metagenomics-novaseq $*
  apply amd-metagenomics-novaseq-control $*
  apply amd-metagenomics-analysed $*
}

fungi()
{
  apply fungi-illumina-shortread $*
  apply fungi-ont-promethion $*
}

pp()
{
  apply pp-illumina-shortread $*
  apply pp-pacbio-hifi $*
  apply pp-ont-promethion $*
}

cipps()
{
  apply cipps-illumina-shortread $*
  apply cipps-pacbio-hifi $*
}

ppa()
{
  apply ppa-phenoct-xray $*
  apply ppa-phenoct-xray-analysed $*
  apply ppa-hyperspectral $*
  apply ppa-asd-spectro $*
  apply ppa-nutritional-analysed $*
  apply ppa-metabolomics $*
  apply ppa-metabolomics-analysed $*
  apply ppa-proteomics $*
  apply ppa-proteomics-analysed $*
  apply ppa-proteomics-database $*
  apply ppa-nutritional-analysis $*
}

grasslands() {
  apply grasslands-hi-c $*
  apply grasslands-pacbio-hifi $*
  apply grasslands-genomics-ddrad $*
  apply grasslands-illumina-shortread $*
}

collaborations()
{
  apply collaborations-metagenomics-novaseq $*
  apply collaborations-ont-promethion $*

}

bsd()
{
    apply bsd-site-images $*
    # apply bsd-ont-promethion $*
    # apply bsd-pacio-hifi $*
}

workshop()
{
    apply workshop-illumina-shortread $*
    apply workshop-pacbio-hifi $*
}

run() {
  apply $*
}

all()
{
  amd $*
  base $*
  marine_microbes $*
  gbr $*
  omg $*
  tsi $*
  gap $*
  sepsis $*
  stemcell $*
  wheat $*
  ausarg $*
  fungi $*
  pp $*
  cipps $*
  ppa $*
  grasslands $*
  collabs $*
  bsd $*
  workshop $*
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
