

from unipath import Path

from ...abstract import BaseMetadata

from ...util import make_logger, bpa_id_to_ckan_name, one, common_values, apply_license
from urllib.parse import urljoin

from glob import glob

from ...libs import ingest_utils
from ...libs.excel_wrapper import make_field_definition as fld
from . import files
from .tracking import (
    BASETrackMetadata)
from .contextual import (
    BASESampleContextual,
    BASENCBIContextual)

import os
import re

logger = make_logger(__name__)


common_context = [BASESampleContextual, BASENCBIContextual]


# fixed read lengths provided by AB at CSIRO
amplicon_read_length = {
    '16S': '300bp',
    'A16S': '300bp',
    'ITS': '300bp',
    '18S': '150bp',
}
common_skip = [
    re.compile(r'^.*_metadata\.xlsx$'),
    re.compile(r'^.*SampleSheet.*'),
    re.compile(r'^.*TestFiles\.exe.*'),
]


def base_amplicon_read_length(amplicon):
    return amplicon_read_length[amplicon]


def build_base_amplicon_linkage(index_linkage, flow_id, index):
    # build linkage, `index_linkage` indicates whether we need
    # to include index in the linkage
    if index_linkage:
        # strip out _ and - as usage inconsistent in pilot data
        return flow_id + '_' + index.replace('-', '').replace('_', '')
    return flow_id


class BASEAmpliconsMetadata(BaseMetadata):
    auth = ('base', 'base')
    organization = 'bpa-base'
    ckan_data_type = 'base-genomics-amplicon'
    omics = 'genomics'
    technology = 'amplicons'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/base/raw/amplicons/',
    ]
    metadata_url_components = ('amplicon', 'facility_code', 'ticket')
    resource_linkage = ('sample_extraction_id', 'amplicon', 'base_amplicon_linkage')
    # pilot data
    index_linkage_spreadsheets = ('BASE_18S_UNSW_A6BRJ_metadata.xlsx',)
    index_linkage_md5s = ('BASE_18S_UNSW_A6BRJ_checksums.md5',)
    # spreadsheet
    spreadsheet = {
        'fields': [
            fld("bpa_id", "Soil sample unique ID", coerce=ingest_utils.extract_bpa_id),
            fld("sample_extraction_id", "Sample extraction ID", coerce=ingest_utils.fix_sample_extraction_id),
            fld("sequencing_facility", "Sequencing facility"),
            fld("target", "Target", coerce=lambda s: s.upper().strip()),
            fld("index", "Index", coerce=lambda s: s[:12]),
            fld("index1", "Index 1", coerce=lambda s: s[:12]),
            fld("index2", "Index2", coerce=lambda s: s[:12]),
            fld("pcr_1_to_10", "1:10 PCR, P=pass, F=fail", coerce=ingest_utils.fix_pcr),
            fld("pcr_1_to_100", "1:100 PCR, P=pass, F=fail", coerce=ingest_utils.fix_pcr),
            fld("pcr_neat", "neat PCR, P=pass, F=fail", coerce=ingest_utils.fix_pcr),
            fld("dilution", "Dilution used", coerce=ingest_utils.fix_date_interval),
            fld("sequencing_run_number", "Sequencing run number"),
            fld("flow_cell_id", "Flowcell"),
            fld("reads", ("# of RAW reads", "# of reads"), coerce=ingest_utils.get_int),
            fld("sample_name", "Sample name on sample sheet"),
            fld("analysis_software_version", "AnalysisSoftwareVersion"),
            fld("comments", "Comments"),
        ],
        'options': {
            'header_length': 2,
            'column_name_row_index': 1,
        }
    }
    md5 = {
        'match': files.amplicon_regexps,
        'skip': common_skip + files.amplicon_control_regexps,
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(BASEAmpliconsMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = BASETrackMetadata()

    def _get_packages(self):
        xlsx_re = re.compile(r'^.*_(\w+)_metadata.*\.xlsx$')

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                raise Exception("unable to find flowcell for filename: `%s'" % (fname))
            return m.groups()[0]

        logger.info("Ingesting BASE Amplicon metadata from {0}".format(self.path))
        packages = []
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing BASE Amplicon metadata file {0}".format(os.path.basename(fname)))
            for row in self.parse_spreadsheet(fname, self.metadata_info):
                track_meta = self.track_meta.get(row.ticket)
                flow_id = get_flow_id(fname)

                def track_get(k):
                    if track_meta is None:
                        return None
                    return getattr(track_meta, k)
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                sample_extraction_id = ingest_utils.make_sample_extraction_id(row.sample_extraction_id, bpa_id)
                base_fname = os.path.basename(fname)
                index_linkage = base_fname in self.index_linkage_spreadsheets
                base_amplicon_linkage = build_base_amplicon_linkage(index_linkage, flow_id, row.index)
                if index_linkage:
                    note_extra = '%s %s' % (flow_id, row.index)
                else:
                    note_extra = flow_id
                obj = {}
                amplicon = row.amplicon.upper()
                name = bpa_id_to_ckan_name(sample_extraction_id, self.ckan_data_type + '-' + amplicon, base_amplicon_linkage)
                archive_ingestion_date = ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive'))

                obj.update({
                    'name': name,
                    'id': name,
                    'sample_type': 'soil',
                    'read_length': base_amplicon_read_length(amplicon),  # hard-coded for now, on advice of AB at CSIRO
                    'bpa_id': bpa_id,
                    'flow_id': flow_id,
                    'base_amplicon_linkage': base_amplicon_linkage,
                    'sample_extraction_id': sample_extraction_id,
                    'target': row.target,
                    'index': row.index,
                    'index1': row.index1,
                    'index2': row.index2,
                    'pcr_1_to_10': row.pcr_1_to_10,
                    'pcr_1_to_100': row.pcr_1_to_100,
                    'pcr_neat': row.pcr_neat,
                    'dilution': row.dilution,
                    'sequencing_run_number': row.sequencing_run_number,
                    'flow_cell_id': row.flow_cell_id,
                    'reads': row.reads,
                    'sample_name': row.sample_name,
                    'analysis_software_version': row.analysis_software_version,
                    'amplicon': amplicon,
                    'notes': 'BASE Amplicons %s %s %s' % (amplicon, sample_extraction_id, note_extra),
                    'title': 'BASE Amplicons %s %s %s' % (amplicon, sample_extraction_id, note_extra),
                    'date_of_transfer': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'data_type': track_get('data_type'),
                    'description': track_get('description'),
                    'folder_name': track_get('folder_name'),
                    'sample_submission_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                    'contextual_data_submission_date': None,
                    'data_generated': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                    'archive_ingestion_date': archive_ingestion_date,
                    'license_id': apply_license(archive_ingestion_date),
                    'dataset_url': track_get('download'),
                    'ticket': row.ticket,
                    'facility': row.facility_code.upper(),
                    'type': self.ckan_data_type,
                    'comments': row.comments,
                    'private': True,
                })
                for contextual_source in self.contextual_metadata:
                    obj.update(contextual_source.get(bpa_id))
                ingest_utils.add_spatial_extra(obj)
                tag_names = ['amplicons', amplicon, obj['sample_type']]
                obj['tags'] = [{'name': t} for t in tag_names]
                packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting BASE Amplicon md5 file information from {0}".format(self.path))
        resources = []

        for md5_file in glob(self.path + '/*.md5'):
            index_linkage = os.path.basename(md5_file) in self.index_linkage_md5s
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_extraction_id = bpa_id.split('.')[-1] + '_' + file_info.get('extraction')
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((sample_extraction_id, resource['amplicon'], build_base_amplicon_linkage(index_linkage, resource['flow_id'], resource['index'])), legacy_url, resource))
        return resources


class BASEAmpliconsControlMetadata(BaseMetadata):
    auth = ('base', 'base')
    organization = 'bpa-base'
    ckan_data_type = 'base-genomics-amplicon-control'
    omics = 'genomics'
    technology = 'amplicons-control'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/base/raw/amplicons/',
    ]
    metadata_url_components = ('amplicon', 'facility_code', 'ticket')
    resource_linkage = ('amplicon', 'flow_id')
    md5 = {
        'match': files.amplicon_control_regexps,
        'skip': common_skip + files.amplicon_regexps,
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(BASEAmpliconsControlMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.metadata_info = metadata_info
        self.contextual_metadata = contextual_metadata
        self.track_meta = BASETrackMetadata()

    def md5_lines(self):
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                yield filename, md5, md5_file, file_info

    def _get_packages(self):
        flow_id_ticket = dict(((t['amplicon'], t['flow_id']), self.metadata_info[os.path.basename(fname)]) for _, _, fname, t in self.md5_lines())
        packages = []
        for (amplicon, flow_id), info in sorted(flow_id_ticket.items()):
            obj = {}
            name = bpa_id_to_ckan_name('control', self.ckan_data_type + '-' + amplicon, flow_id).lower()
            track_meta = self.track_meta.get(info['ticket'])

            def track_get(k):
                if track_meta is None:
                    return None
                return getattr(track_meta, k)

            archive_ingestion_date = ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive'))

            obj.update({
                'name': name,
                'id': name,
                'flow_id': flow_id,
                'notes': 'BASE Amplicons Control %s %s' % (amplicon, flow_id),
                'title': 'BASE Amplicons Control %s %s' % (amplicon, flow_id),
                'read_length': base_amplicon_read_length(amplicon),  # hard-coded for now, on advice of AB at CSIRO
                'omics': 'Genomics',
                'analytical_platform': 'MiSeq',
                'date_of_transfer': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                'data_type': track_get('data_type'),
                'description': track_get('description'),
                'folder_name': track_get('folder_name'),
                'sample_submission_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
                'contextual_data_submission_date': None,
                'data_generated': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
                'archive_ingestion_date': archive_ingestion_date,
                'license_id': apply_license(archive_ingestion_date),
                'dataset_url': track_get('download'),
                'ticket': info['ticket'],
                'facility': info['facility_code'].upper(),
                'amplicon': amplicon,
                'type': self.ckan_data_type,
                'private': True,
            })
            ingest_utils.add_spatial_extra(obj)
            tag_names = ['amplicons-control', amplicon, 'raw']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        resources = []
        for filename, md5, md5_file, file_info in self.md5_lines():
            resource = file_info.copy()
            resource['md5'] = resource['id'] = md5
            resource['name'] = filename
            resource['resource_type'] = self.ckan_data_type
            for contextual_source in self.contextual_metadata:
                resource.update(contextual_source.filename_metadata(filename))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            legacy_url = urljoin(xlsx_info['base_url'], filename)
            resources.append(((resource['amplicon'], resource['flow_id']), legacy_url, resource))
        return resources


class BASEMetagenomicsMetadata(BaseMetadata):
    auth = ('base', 'base')
    organization = 'bpa-base'
    ckan_data_type = 'base-metagenomics'
    omics = 'metagenomics'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$', r'^.*_metadata.*.*\.xlsx$']
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/base/raw/metagenomics/',
    ]
    metadata_url_components = ('facility_code', 'ticket')
    resource_linkage = ('sample_extraction_id', 'flow_id')
    spreadsheet = {
        'fields': [
            fld('bpa_id', 'Soil sample unique ID', coerce=ingest_utils.extract_bpa_id),
            fld('sample_extraction_id', 'Sample extraction ID', coerce=ingest_utils.fix_sample_extraction_id),
            fld('insert_size_range', 'Insert size range'),
            fld('library_construction_protocol', 'Library construction protocol'),
            fld('sequencer', 'Sequencer'),
            fld('casava_version', 'CASAVA version'),
            fld('flow_cell_id', 'Run #:Flow Cell ID'),
        ],
        'options': {
            'header_length': 2,
            'column_name_row_index': 1,
        }
    }
    md5 = {
        'match': files.metagenomics_regexps,
        'skip': common_skip,
    }
    # these are packages from the pilot, which have missing metadata
    # we synthethise minimal packages for this data - see
    # https://github.com/muccg/bpa-archive-ops/issues/140
    missing_packages = [
        ('8154_2', 'H9BB6ADXX'),
        ('8158_2', 'H9BB6ADXX'),
        ('8159_2', 'H9BB6ADXX'),
        ('8262_2', 'H9EV8ADXX'),
        ('8263_2', 'H9BB6ADXX'),
        ('8268_2', 'H80EYADXX'),
        ('8268_2', 'H9EV8ADXX'),
        ('8269_2', 'H80EYADXX'),
        ('8269_2', 'H9EV8ADXX'),
        ('8270_2', 'H80EYADXX'),
        ('8271_2', 'H80EYADXX'),
        ('8271_2', 'H9EV8ADXX')]

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(BASEMetagenomicsMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.track_meta = BASETrackMetadata()

    def assemble_obj(self, bpa_id, sample_extraction_id, flow_id, row, track_meta):
        def track_get(k):
            if track_meta is None:
                return None
            return getattr(track_meta, k)

        def row_get(k, v_fn=None):
            if row is None:
                return None
            res = getattr(row, k)
            if v_fn is not None:
                res = v_fn(res)
            return res

        name = bpa_id_to_ckan_name(sample_extraction_id, self.ckan_data_type, flow_id)
        archive_ingestion_date = ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive'))

        obj = {
            'name': name,
            'sample_type': 'soil',
            'id': name,
            'bpa_id': bpa_id,
            'flow_id': flow_id,
            'read_length': '150bp',  # hard-coded for now, on advice of AB at CSIRO
            'sample_extraction_id': sample_extraction_id,
            'insert_size_range': row_get('insert_size_range'),
            'library_construction_protocol': row_get('library_construction_protocol'),
            'sequencer': row_get('sequencer'),
            'analysis_software_version': row_get('casava_version'),
            'notes': 'BASE Metagenomics %s' % (sample_extraction_id),
            'title': 'BASE Metagenomics %s' % (sample_extraction_id),
            'date_of_transfer': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
            'data_type': track_get('data_type'),
            'description': track_get('description'),
            'folder_name': track_get('folder_name'),
            'sample_submission_date': ingest_utils.get_date_isoformat(track_get('date_of_transfer')),
            'contextual_data_submission_date': None,
            'data_generated': ingest_utils.get_date_isoformat(track_get('date_of_transfer_to_archive')),
            'archive_ingestion_date': archive_ingestion_date,
            'license_id': apply_license(archive_ingestion_date),
            'dataset_url': track_get('download'),
            'ticket': row_get('ticket'),
            'facility': row_get('facility_code', lambda v: v.upper()),
            'type': self.ckan_data_type,
            'private': True,
        }
        for contextual_source in self.contextual_metadata:
            obj.update(contextual_source.get(bpa_id))
        ingest_utils.add_spatial_extra(obj)
        tag_names = ['metagenomics', obj['sample_type']]
        obj['tags'] = [{'name': t} for t in tag_names]
        return obj

    def _get_packages(self):
        xlsx_re = re.compile(r'^.*_([A-Z0-9]{9})_metadata.*\.xlsx$')

        def get_flow_id(fname):
            m = xlsx_re.match(fname)
            if not m:
                logger.warning("unable to find flowcell for filename: `%s'" % (fname))
                return None
            return m.groups()[0]

        logger.info("Ingesting BASE Metagenomics metadata from {0}".format(self.path))
        packages = []

        # missing metadata (see note above)
        for sample_extraction_id, flow_id in self.missing_packages:
            bpa_id = ingest_utils.extract_bpa_id(sample_extraction_id.split('_')[0])
            sample_extraction_id = ingest_utils.make_sample_extraction_id(sample_extraction_id, bpa_id)
            md5_file = one(glob(self.path + '/*%s*.md5' % (flow_id)))
            xlsx_info = self.metadata_info[os.path.basename(md5_file)]
            track_meta = self.track_meta.get(xlsx_info['ticket'])
            packages.append(self.assemble_obj(bpa_id, sample_extraction_id, flow_id, None, track_meta))

        # the generated package IDs will have duplicates, due to data issues in the pilot data
        # we simply skip over the duplicates, which don't have any significant data differences
        generated_packages = set()
        for fname in glob(self.path + '/*.xlsx'):
            logger.info("Processing BASE Metagenomics metadata file {0}".format(os.path.basename(fname)))
            # unique the rows, duplicates in some of the sheets
            uniq_rows = set(t for t in self.parse_spreadsheet(fname, self.metadata_info))
            for row in uniq_rows:
                track_meta = self.track_meta.get(row.ticket)
                # pilot data has the flow cell in the spreadsheet; in the main dataset
                # there is one flow-cell per spreadsheet, so it's in the spreadsheet
                # filename
                flow_id = row.flow_cell_id
                if flow_id is None:
                    flow_id = get_flow_id(fname)
                if flow_id is None:
                    raise Exception("can't determine flow_id for %s / %s" % (fname, repr(row)))
                bpa_id = row.bpa_id
                if bpa_id is None:
                    continue
                sample_extraction_id = ingest_utils.make_sample_extraction_id(row.sample_extraction_id, bpa_id)
                new_obj = self.assemble_obj(bpa_id, sample_extraction_id, flow_id, row, track_meta)
                if new_obj['id'] in generated_packages:
                    logger.debug('skipped attempt to generate duplicate package: %s' % new_obj['id'])
                    continue
                generated_packages.add(new_obj['id'])
                packages.append(new_obj)

        return packages

    def _get_resources(self):
        logger.info("Ingesting BASE Metagenomics md5 file information from {0}".format(self.path))
        resources = []
        for md5_file in glob(self.path + '/*.md5'):
            logger.info("Processing md5 file {}".format(md5_file))
            for filename, md5, file_info in self.parse_md5file(md5_file):
                bpa_id = ingest_utils.extract_bpa_id(file_info.get('id'))
                resource = file_info.copy()
                resource['md5'] = resource['id'] = md5
                resource['name'] = filename
                resource['resource_type'] = self.ckan_data_type
                for contextual_source in self.contextual_metadata:
                    resource.update(contextual_source.filename_metadata(filename))
                sample_extraction_id = bpa_id.split('.')[-1] + '_' + file_info.get('extraction')
                xlsx_info = self.metadata_info[os.path.basename(md5_file)]
                legacy_url = urljoin(xlsx_info['base_url'], filename)
                resources.append(((sample_extraction_id, resource['flow_id']), legacy_url, resource))
        return resources


class BASESiteImagesMetadata(BaseMetadata):
    auth = ('base', 'base')
    organization = 'bpa-base'
    ckan_data_type = 'base-site-image'
    contextual_classes = common_context
    metadata_patterns = [r'^.*\.md5$']
    omics = None
    technology = 'site-images'
    metadata_urls = [
        'https://downloads-qcif.bioplatforms.com/bpa/base/site-images/',
    ]
    metadata_url_components = ('ticket',)
    resource_linkage = ('site_ids',)
    md5 = {
        'match': [
            files.site_image_filename_re
        ],
        'skip': None,
    }

    def __init__(self, metadata_path, contextual_metadata=None, metadata_info=None):
        super(BASESiteImagesMetadata, self).__init__()
        self.path = Path(metadata_path)
        self.contextual_metadata = contextual_metadata
        self.metadata_info = metadata_info
        self.id_to_resources = self._read_md5s()

    def _read_md5s(self):
        id_to_resources = {}
        for fname in glob(self.path + '/*.md5'):
            logger.info("Processing MD5 file %s" % (fname))
            xlsx_info = self.metadata_info[os.path.basename(fname)]
            for filename, md5, file_info in self.parse_md5file(fname):
                id_tpl = (file_info['id1'], file_info['id2'])
                assert(id_tpl not in id_to_resources)
                obj = {
                    'md5': md5,
                    'filename': filename
                }
                obj.update(xlsx_info)
                id_to_resources[id_tpl] = obj
        return id_to_resources

    @classmethod
    def id_tpl_to_site_ids(cls, id_tpl):
        return ', '.join([ingest_utils.extract_bpa_id(t) for t in id_tpl])

    def _get_packages(self):
        logger.info("Ingesting BPA BASE Images metadata from {0}".format(self.path))
        packages = []
        for id_tpl in sorted(self.id_to_resources):
            info = self.id_to_resources[id_tpl]
            obj = {}
            name = bpa_id_to_ckan_name('%s-%s' % id_tpl, self.ckan_data_type).lower()
            # find the common contextual metadata for the site IDs
            context = []
            for abbrev in id_tpl:
                fragment = {}
                for contextual_source in self.contextual_metadata:
                    fragment.update(contextual_source.get(ingest_utils.extract_bpa_id(abbrev)))
                context.append(fragment)
            obj.update(common_values(context))
            obj.update({
                'name': name,
                'id': name,
                'site_ids': self.id_tpl_to_site_ids(id_tpl),
                'title': 'BASE Site Image %s %s' % id_tpl,
                'notes': 'Site image: %s' % (obj['location_description']),
                'omics': 'Genomics',
                'analytical_platform': 'MiSeq',
                'ticket': info['ticket'],
                'type': self.ckan_data_type,
                'private': True,
            })
            ingest_utils.add_spatial_extra(obj)
            tag_names = ['site-images']
            obj['tags'] = [{'name': t} for t in tag_names]
            packages.append(obj)
        return packages

    def _get_resources(self):
        logger.info("Ingesting Sepsis md5 file information from {0}".format(self.path))
        resources = []
        for id_tpl in sorted(self.id_to_resources):
            site_ids = self.id_tpl_to_site_ids(id_tpl)
            info = self.id_to_resources[id_tpl]
            resource = {}
            resource['md5'] = resource['id'] = info['md5']
            filename = info['filename']
            resource['name'] = filename
            legacy_url = urljoin(info['base_url'], filename)
            resources.append(((site_ids,), legacy_url, resource))
        return resources
