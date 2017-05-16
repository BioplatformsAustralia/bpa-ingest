from __future__ import print_function

from unipath import Path
from urlparse import urljoin
from glob import glob

from ...util import make_logger, bpa_id_to_ckan_name
from ...libs import ingest_utils
from ...abstract import BaseMetadata
from ...libs.excel_wrapper import ExcelWrapper
from . import files
from . tracking import MarineMicrobesTrackMetadata
from .contextual import MarineMicrobesSampleContextual

import os
import re


logger = make_logger(__name__)

index_from_comment_re = re.compile(r'([G|A|T|C|-]{6,}_[G|A|T|C|-]{6,})')
index_from_comment_pilot_re = re.compile(r'_([G|A|T|C|-]{6,})_')


def index_from_comment(attrs):
    # return the index from a comment (for linkage on pilot data)
    # 34865_1_18S_UNSW_ATCTCAGG_GTAAGGAG_AWMVL
    # 21644_16S_UNSW_GTCAATTGACCG_AFGB7
    # 21644_A16S_UNSW_CGGAGCCT_TCGACTAG_AG27L
    for attr in attrs:
        if not attr:
            continue
        m = index_from_comment_re.search(attr)
        if not m:
            m = index_from_comment_pilot_re.search(attr)
        if not m:
            continue
        return m.groups()[0]


def build_mm_amplicon_linkage(index_linkage, flow_id, index):
    # build linkage, `index_linkage` indicates whether we need
    # to include index in the linkage
    if index_linkage:
        # strip out _ and - as usage inconsistent in pilot data
        return flow_id + '_' + index.replace('-', '').replace('_', '')
    return flow_id


def unique_spreadsheets(fnames):
    # project manager is updating submission sheets to correct errors
    # we want to keep the originals in case of any problems, so override
    # original with the update
    update_files = [t for t in fnames if '_UPDATE' in t]
    skip_files = set()
    for fname in update_files:
        skip_files.add(fname.replace('_UPDATE', ''))
    return [t for t in fnames if t not in skip_files]


class BaseMarineMicrobesAmpliconsMetadata(BaseMetadata):
    auth = ('marine', 'marine')
    organization = 'bpa-marine-microbes'
    ckan_data_type = 'mm-genomics-amplicon'
    contextual_classes = [MarineMicrobesSampleContextual]
    metadata_patterns = [r'^.*\.md5', r'^.*_metadata.*.*\.xlsx']
    resource_linkage = ('bpa_id', 'mm_amplicon_linkage')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = MarineMicrobesTrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            ("bpa_id", re.compile(r'^.*sample unique id$'), ingest_utils.extract_bpa_id),
            ("sample_extraction_id", "Sample extraction ID"),
            ("target", "Target"),
            ("dilution_used", "Dilution used", ingest_utils.fix_date_interval),
            ("reads", re.compile(r"^# of (raw )?reads$")),
            ("analysis_software_version", "AnalysisSoftwareVersion"),
            ("comments", "Comments"),
            ("sample_name_on_sample_sheet", "Sample name on sample sheet"),
            # special case: we merge these together (and throw a hard error if more than one has data for a given row)
            ("pass_fail", "P=pass, F=fail"),
            ("pass_fail_neat", "1:10 PCR, P=pass, F=fail", None, True),
            ("pass_fail_10", "1:100 PCR, P=pass, F=fail", None, True),
            ("pass_fail_100", "neat PCR, P=pass, F=fail", None, True),
        ]
        try:
            wrapper = ExcelWrapper(
                field_spec,
                fname,
                sheet_name=None,
                header_length=2,
                column_name_row_index=1,
                formatting_info=True,
                additional_context=metadata_info[os.path.basename(fname)])
            rows = list(wrapper.get_all())
            return rows
        except:
            logger.error("Cannot parse: `%s'" % (fname))
            return []

    def get_packages(self):
        xlsx_re = re.compile(r'^.*_(\w+)_metadata.*\.xlsx$')

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        logger.info("Ingesting Marine Microbes Transcriptomics metadata from {0}".format(self.path))
        packages = []
        for fname in unique_spreadsheets(glob(self.path + '/*.xlsx')):
            base_fname = os.path.basename(fname)
            logger.info("Processing Marine Microbes Transcriptomics metadata file {0}".format(os.path.basename(fname)))
            flow_id = get_flow_id(fname)
            # the pilot data needs increased linkage, due to multiple trials on the same BPA ID
            index_linkage = base_fname in self.index_linkage_spreadsheets
            for row in BaseMarineMicrobesAmpliconsMetadata.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                obj = {}
                index = index_from_comment([row.comments, row.sample_name_on_sample_sheet])
                mm_amplicon_linkage = build_mm_amplicon_linkage(index_linkage, flow_id, index)
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type + '-' + self.amplicon, mm_amplicon_linkage)
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'flow_id': flow_id,
                    'mm_amplicon_linkage': mm_amplicon_linkage,
                    'sample_extraction_id': ingest_utils.make_sample_extraction_id(row.sample_extraction_id, bpa_id),
                    'target': row.target,
                    'pass_fail': ingest_utils.merge_pass_fail(row),
                    'dilution_used': row.dilution_used,
                    'reads': row.reads,
                    'analysis_software_version': row.analysis_software_version,
                    'amplicon': self.amplicon,
                    'notes': 'Marine Microbes Amplicons %s %s %s' % (self.amplicon, bpa_id, flow_id),
                    'title': 'Marine Microbes Amplicons %s %s %s' % (self.amplicon, bpa_id, flow_id),
                    'omics': 'Genomics',
                    'analytical_platform': 'MiSeq',
                    'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                    'data_type': track_meta.data_type,
                    'description': track_meta.description,
                    'folder_name': track_meta.folder_name,
                    'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                    'contextual_data_submission_date': None,
                    'data_generated': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                    'dataset_url': track_meta.download,
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'type': self.ckan_data_type,
                    'comments': row.comments,
                    'private': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id))
                ingest_utils.add_spatial_extra(obj)
                tag_names = ['amplicons', self.amplicon]
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting MM md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            index_linkage = os.path.basename(md5_file) in self.index_linkage_md5s
            logger.info("Processing md5 file {} {}".format(md5_file, index_linkage))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.amplicon_filename_re):
                if file_info is None:
                    if not files.amplicon_control_filename_re.match(filename):
                        logger.debug("unable to parse filename: `%s'" % (filename))
                    continue
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_id, build_mm_amplicon_linkage(index_linkage, resource['flow_id'], resource['index'])), legacy_url, resource))
        return resources


class MarineMicrobesGenomicsAmplicons16SMetadata(BaseMarineMicrobesAmpliconsMetadata):
    amplicon = '16s'
    index_linkage_spreadsheets = ('MM_Pilot_1_16S_UNSW_AFGB7_metadata.xlsx',)
    index_linkage_md5s = ('MM_1_16S_UNSW_AFGB7_checksums.md5',)
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/amplicons/16s/'
    ]
    metadata_url_components = ('facility_code', 'ticket')


class MarineMicrobesGenomicsAmpliconsA16SMetadata(BaseMarineMicrobesAmpliconsMetadata):
    amplicon = 'a16s'
    index_linkage_spreadsheets = ('MM-Pilot_A16S_UNSW_AG27L_metadata_UPDATE.xlsx',)
    index_linkage_md5s = ('MM_Pilot_A16S_UNSW_AG27L_checksums.md5',)
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/amplicons/a16s/'
    ]
    metadata_url_components = ('facility_code', 'ticket')


class MarineMicrobesGenomicsAmplicons18SMetadata(BaseMarineMicrobesAmpliconsMetadata):
    amplicon = '18s'
    index_linkage_spreadsheets = ('MM_Pilot_18S_UNSW_AGGNB_metadata.xlsx',)
    index_linkage_md5s = ('MM_18S_UNSW_AGGNB_checksums.md5',)
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/amplicons/18s/'
    ]
    metadata_url_components = ('facility_code', 'ticket')


class BaseMarineMicrobesAmpliconsControlMetadata(BaseMetadata):
    auth = ('marine', 'marine')
    organization = 'bpa-marine-microbes'
    ckan_data_type = 'mm-genomics-amplicon-control'
    contextual_classes = []
    metadata_patterns = [r'^.*\.md5']
    resource_linkage = ('amplicon', 'flow_id')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info
        self.track_meta = MarineMicrobesTrackMetadata(track_csv_path)

    def md5_lines(self):
        logger.info("Ingesting MM md5 file information from {0}".format(self.path))
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.amplicon_control_filename_re):
                if file_info is None:
                    if not files.amplicon_filename_re.match(filename):
                        logger.debug("unable to parse filename: `%s'" % (filename))
                    continue

                yield filename, md5, md5_file, file_info

    def get_packages(self):
        flow_id_ticket = dict((t['flow_id'], self.metadata_info[os.path.basename(fname)]) for _, _, fname, t in self.md5_lines())
        packages = []
        for flow_id, info in sorted(flow_id_ticket.items()):
            obj = {}
            name = bpa_id_to_ckan_name('control', self.ckan_data_type + '-' + self.amplicon, flow_id).lower()
            track_meta = self.track_meta.get(info['ticket'])
            obj.update({
                'name': name,
                'id': name,
                'flow_id': flow_id,
                'notes': 'Marine Microbes Amplicons Control %s %s' % (self.amplicon, flow_id),
                'title': 'Marine Microbes Amplicons Control %s %s' % (self.amplicon, flow_id),
                'omics': 'Genomics',
                'analytical_platform': 'MiSeq',
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'ticket': info['ticket'],
                'facility': info['facility_code'].upper(),
                'amplicon': self.amplicon,
                'type': self.ckan_data_type,
                'private': True,
            })
            ingest_utils.add_spatial_extra(obj)
            tag_names = ['amplicons-control', self.amplicon, 'raw']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_resources(self):
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = file_info.copy()
            resource['md5'] = resource['id'] = md5
            resource['name'] = filename
            resource['resource_type'] = self.ckan_data_type
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info['base_url'], filename)
            resources.append(((self.amplicon, resource['flow_id']), legacy_url, resource))
        return resources


class MarineMicrobesGenomicsAmplicons16SControlMetadata(BaseMarineMicrobesAmpliconsControlMetadata):
    amplicon = '16s'
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/amplicons/16s/'
    ]
    metadata_url_components = ('facility_code', 'ticket')


class MarineMicrobesGenomicsAmpliconsA16SControlMetadata(BaseMarineMicrobesAmpliconsControlMetadata):
    amplicon = 'a16s'
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/amplicons/a16s/'
    ]
    metadata_url_components = ('facility_code', 'ticket')


class MarineMicrobesGenomicsAmplicons18SControlMetadata(BaseMarineMicrobesAmpliconsControlMetadata):
    amplicon = '18s'
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/amplicons/18s/'
    ]
    metadata_url_components = ('facility_code', 'ticket')


class MarineMicrobesMetagenomicsMetadata(BaseMetadata):
    auth = ('marine', 'marine')
    organization = 'bpa-marine-microbes'
    ckan_data_type = 'mm-metagenomics'
    contextual_classes = [MarineMicrobesSampleContextual]
    metadata_patterns = [r'^.*\.md5', r'^.*_metadata.*\.xlsx']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/metagenomics/'
    ]
    metadata_url_components = ('facility_code', 'ticket')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = MarineMicrobesTrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            ("bpa_id", re.compile(r'^.*sample unique id$'), ingest_utils.extract_bpa_id),
            ("sample_extraction_id", "Sample extraction ID", None, True),
            ("insert_size_range", "Insert size range"),
            ("library_construction_protocol", "Library construction protocol"),
            ("sequencer", "Sequencer"),
            ("analysis_software_version", ("casava version", "bcl2fastq2", re.compile(r'^software[ &]+version$'))),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=metadata_info[os.path.basename(fname)])
        rows = list(wrapper.get_all())
        return rows

    def get_packages(self):
        logger.info("Ingesting Marine Microbes Transcriptomics metadata from {0}".format(self.path))
        packages = []
        for fname in unique_spreadsheets(glob(self.path + '/*.xlsx')):
            logger.info("Processing Marine Microbes Transcriptomics metadata file {0}".format(os.path.basename(fname)))
            for row in MarineMicrobesMetagenomicsMetadata.parse_spreadsheet(fname, self.metadata_info):
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                track_meta = self.track_meta.get(row.ticket)
                obj = {}
                name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
                obj.update({
                    'name': name,
                    'id': name,
                    'bpa_id': bpa_id,
                    'notes': 'Marine Microbes Metagenomics %s' % (bpa_id),
                    'title': 'Marine Microbes Metagenomics %s' % (bpa_id),
                    'omics': 'metagenomics',
                    'analytical_platform': 'HiSeq',
                    'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                    'data_type': track_meta.data_type,
                    'description': track_meta.description,
                    'folder_name': track_meta.folder_name,
                    'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                    'contextual_data_submission_date': None,
                    'data_generated': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                    'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                    'dataset_url': track_meta.download,
                    'sample_extraction_id': ingest_utils.make_sample_extraction_id(row.sample_extraction_id, bpa_id),
                    'insert_size_range': row.insert_size_range,
                    'library_construction_protocol': row.library_construction_protocol,
                    'sequencer': row.sequencer,
                    'analysis_software_version': row.analysis_software_version,
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'type': self.ckan_data_type,
                    'private': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id))
                ingest_utils.add_spatial_extra(obj)
                tag_names = ['metagenomics', 'raw']
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting MM md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.metagenomics_filename_re):
                if file_info is None:
                    logger.warning("unable to parse filename: `%s'" % (filename))
                    continue
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_id,), legacy_url, resource))
        return resources


class MarineMicrobesMetatranscriptomeMetadata(BaseMetadata):
    auth = ('marine', 'marine')
    organization = 'bpa-marine-microbes'
    ckan_data_type = 'mm-metatranscriptome'
    contextual_classes = [MarineMicrobesSampleContextual]
    metadata_patterns = [r'^.*\.md5', r'^.*_metadata.*\.xlsx']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/marine_microbes/raw/metatranscriptome/'
    ]
    metadata_url_components = ('facility_code', 'ticket')

    def __init__(self, metadata_path, contextual_metadata=None, track_csv_path=None, metadata_info=None):
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = MarineMicrobesTrackMetadata(track_csv_path)

    @classmethod
    def parse_spreadsheet(self, fname, metadata_info):
        field_spec = [
            ("bpa_id", re.compile(r'^.*sample unique id$'), ingest_utils.extract_bpa_id),
            ("sample_extraction_id", "Sample extraction ID", None),
            ("insert_size_range", "Insert size range", None),
            ("library_construction_protocol", "Library construction protocol", None),
            ("sequencer", "Sequencer", None),
            ("analysis_software_version", "CASAVA version", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True,
            additional_context=metadata_info[os.path.basename(fname)])
        rows = list(wrapper.get_all())
        return rows

    def get_packages(self):
        logger.info("Ingesting Marine Microbes Transcriptomics metadata from {0}".format(self.path))
        packages = []
        # duplicate rows are an issue in this project. we filter them out by uniquifying
        # this is harmless as they have to precisly match, and BPA_ID is the primary key
        all_rows = set()
        for fname in unique_spreadsheets(glob(self.path + '/*.xlsx')):
            logger.info("Processing Marine Microbes Transcriptomics metadata file {0}".format(os.path.basename(fname)))
            for row in MarineMicrobesMetatranscriptomeMetadata.parse_spreadsheet(fname, self.metadata_info):
                all_rows.add(row)
        for row in sorted(all_rows):
            bpa_id = row.bpa_id
            if bpa_id is None:
                continue
            track_meta = self.track_meta.get(row.ticket)
            obj = {}
            name = bpa_id_to_ckan_name(bpa_id.split('.')[-1], self.ckan_data_type)
            obj.update({
                'name': name,
                'id': name,
                'bpa_id': bpa_id,
                'notes': 'Marine Microbes Metatranscriptome %s' % (bpa_id),
                'title': 'Marine Microbes Metatranscriptome %s' % (bpa_id),
                'omics': 'metatranscriptomics',
                'analytical_platform': 'HiSeq',
                'date_of_transfer': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'data_type': track_meta.data_type,
                'description': track_meta.description,
                'folder_name': track_meta.folder_name,
                'sample_submission_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'archive_ingestion_date': ingest_utils.get_date_isoformat(track_meta.date_of_transfer_to_archive),
                'dataset_url': track_meta.download,
                'sample_extraction_id': ingest_utils.make_sample_extraction_id(row.sample_extraction_id, bpa_id),
                'insert_size_range': row.insert_size_range,
                'library_construction_protocol': row.library_construction_protocol,
                'sequencer': row.sequencer,
                'analysis_software_version': row.analysis_software_version,
                'ticket': row.ticket,
                'facility': row.facility_code.upper(),
                'type': self.ckan_data_type,
                'private': True,
            })
            for contextual_source in self.contextual_metadata:
                obj.update(contextual_source.get(bpa_id))
            ingest_utils.add_spatial_extra(obj)
            tag_names = ['metatranscriptome', 'raw']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def get_resources(self):
        logger.info("Ingesting MM md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {0}".format(md5_file))
            for filename, md5, file_info in files.parse_md5_file(md5_file, files.metatranscriptome_filename_re):
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((bpa_id,), legacy_url, resource))
        return resources
