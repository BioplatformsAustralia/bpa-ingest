import re
from ...util import make_logger, one
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper
from .files import proteomics_raw_extract_pool_id
from glob import glob


def bpaops_clean(s):
    return s.lower().replace('-', '')


logger = make_logger(__name__)


class StemcellsAGRFTranscriptomeContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-06-07/']
    name = 'stemcell-agrf-transcriptome'
    sheet_name = 'AGRF-Transcriptome'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, bpa_id))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.bpa_id is None:
                continue
            assert(row.bpa_id not in sample_metadata)
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'bpa_id':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('dataset_id', 'data set id'),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id),
            ('number_samples', 'number samples'),
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work order'),
            ('omics_type', 'omics type'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops'),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('dataset', 'data set'),
            ('species', 'species'),
            ('sample_description', 'sample description'),
            ('tissue', 'tissue'),
            ('cell_type', 'cell type'),
            ('disease_state', 'disease state'),
            ('growth_protocol', 'growth protocol'),
            ('treatment_protocol', 'treatment protocol'),
            ('extract_protocol', 'extract protocol'),
            ('library_strategy', 'library strategy'),
            ('date', 'date', ingest_utils.get_date_isoformat),
            ('insert_size_range', 'insert size range'),
            ('library_construction_protocol', 'library construction protocol'),
            ('platform', 'platform'),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        return wrapper.get_all()


class StemcellsAGRFsmRNAContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-06-07/']
    name = 'stemcell-agrf-smallrna'
    sheet_name = 'AGRF-smRNA'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, bpa_id))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.bpa_id is None:
                continue
            assert(row.bpa_id not in sample_metadata)
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'bpa_id':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('dataset_id', 'data set id'),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id),
            ('total_samples', 'total samples'),
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work order'),
            ('omics_type', 'omics type'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops'),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('dataset', 'data set'),
            ('species', 'species'),
            ('sample_description', 'sample description'),
            ('tissue', 'tissue'),
            ('cell_type', 'cell type'),
            ('disease_state', 'disease state'),
            ('growth_protocol', 'growth protocol'),
            ('treatment_protocol', 'treatment protocol'),
            ('extract_protocol', 'extract protocol'),
            ('library_strategy', 'library strategy'),
            ('date', 'date', ingest_utils.get_date_isoformat),
            ('insert_size_range', 'insert size range'),
            ('library_construction_protocol', 'library construction protocol'),
            ('platform', 'platform'),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        return wrapper.get_all()


class StemcellsRamaciottiSingleCell(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-06-07/']
    name = 'stemcell-ramaciotti-singlecell'
    sheet_name = 'Ramaciotti-Single Cell '

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, bpa_id))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.bpa_id is None:
                continue
            assert(row.bpa_id not in sample_metadata)
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'bpa_id':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('dataset_id', 'data set id'),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id),
            ('total_samples', 'total samples'),
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work order'),
            ('omics_type', 'omics type'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops'),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('dataset', 'data set'),
            ('species', 'species'),
            ('sample_description', 'sample description'),
            ('tissue', 'tissue'),
            ('cell_type', 'cell type'),
            ('disease_state', 'disease state'),
            ('growth_protocol', 'growth protocol'),
            ('treatment_protocol', 'treatment protocol'),
            ('extract_protocol', 'extract protocol'),
            ('library_strategy', 'library strategy'),
            ('date', 'date', ingest_utils.get_date_isoformat),
            ('insert_size_range', 'insert size range'),
            ('library_construction_protocol', 'library construction protocol'),
            ('platform', 'platform'),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        return wrapper.get_all()


class StemcellsMetabolomicsContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-06-07/']
    name = 'stemcell-metabolomics'
    sheet_name = 'Metabolomics_Raw'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id, analytical_platform):
        tpl = (bpa_id, analytical_platform)
        if tpl in self.sample_metadata:
            return self.sample_metadata[tpl]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, tpl))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.bpa_id is None:
                continue
            tpl = (row.bpa_id, row.analytical_platform)
            assert(tpl not in sample_metadata)
            sample_metadata[tpl] = row_meta = {}
            for field in row._fields:
                if field != 'bpa_id' and field != 'analytical_platform':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        def fix_analytical_platform(s):
            return s.replace('/', '-')
        field_spec = [
            ('dataset_id', 'data set id'),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id),
            ('total_samples', 'total samples'),
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work order'),
            ('omics_type', 'omics type'),
            ('analytical_platform', 'analytical platform', fix_analytical_platform),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops'),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('dataset', 'data set'),
            ('species', 'species'),
            ('sample_description', 'sample_description'),
            ('tissue', 'tissue'),
            ('cell_type', 'cell type'),
            ('disease_state', 'disease state'),
            ('organism_part', 'organism part'),
            ('growth_protocol', 'growth protocol'),
            ('extract_protocol', 'extract protocol'),
            ('date_data_transferred', 'date data transferred', ingest_utils.get_date_isoformat),
            ('sample_fractionation_extraction_solvent', 'sample fractionation / extraction solvent'),
            ('platform', 'platform'),
            ('instrument_column_type', 'instrument/column type'),
            ('method', 'method'),
            ('mass_spectrometer', 'mass spectrometer'),
            ('acquisition_mode', 'acquisition mode'),
            ('raw_file_name', 'raw file name (available in .d and .mzml format)'),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        return wrapper.get_all()


class StemcellsProteomicsRawContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-06-07/']
    name = 'stemcell-apaf-raw'
    sheet_names = ['APAF-Raw', 'MBPF-Raw']

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id_or_pool_id):
        if bpa_id_or_pool_id in self.sample_metadata:
            return self.sample_metadata[bpa_id_or_pool_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, bpa_id_or_pool_id))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            row_meta = {}
            for field in row._fields:
                if field != 'bpa_id' and field != 'pool_id':
                    row_meta[field] = getattr(row, field)
            if row.bpa_id:
                assert(row.bpa_id not in sample_metadata)
                sample_metadata[row.bpa_id] = row_meta
            if row.pool_id:
                assert(row.pool_id not in sample_metadata)
                sample_metadata[row.pool_id] = row_meta
        return sample_metadata

    def _read_metadata(self, metadata_path):
        # regexps are to cope with slightly different formatting between the two tabs
        # we also have a mix of pool and bpa-id metadata
        field_spec = [
            ('dataset_id', 'data set id'),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id_silent),
            ('pool_id', 'raw file name', proteomics_raw_extract_pool_id),
            ('total_samples', 'total samples'),
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work order'),
            ('omics_type', 'omics type'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops'),
            ('sample_name', re.compile(r'^sample name.*')),
            ('species', re.compile(r'^species.*')),
            ('sample_description', re.compile(r'^sample_description')),
            ('sample_type', re.compile(r'^sample type.*')),
            ('passage_number', re.compile(r'^passage number.*')),
            ('other_protein_sequences', re.compile(r'^other protein sequences.*')),
            ('tissue', re.compile('^tissue.*')),
            ('cell_type', re.compile(r'^cell type.*')),
            ('disease_state', 'disease state'),
            ('labelling', 'labelling'),
            ('growth_protocol', 'growth protocol**'),
            ('treatment_protocol', 'treatment protocol**'),
            ('extract_protocol', 'extract protocol'),
            ('date_data_transferred', 'date data transferred', ingest_utils.get_date_isoformat),
        ]
        rows = []
        for sheet_name in self.sheet_names:
            wrapper = ExcelWrapper(
                field_spec,
                metadata_path,
                sheet_name=sheet_name,
                header_length=2,
                column_name_row_index=1,
                formatting_info=True)
            rows += list(wrapper.get_all())
        # there are a bunch of duplicate rows, because per-file metadata is included
        # in the source spreadsheet
        #
        # we don't need that, so we simply don't include the per-file metadata in
        # `field_spec`, which means we can discard the duplicates here
        return list(set(rows))


class StemcellsProteomicsAnalysedContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-06-07/']
    name = 'stemcell-apaf-analysed'
    sheet_names = ['APAF-Analysed', 'MBPF-Analysed']

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, folder_name):
        # we have to do substring matching here, this is an area for improvement to be flagged with
        # the project manager: we should move to using dataset_id as the matcher
        for k in self.sample_metadata.keys():
            if k.find(folder_name) != -1:
                return self.sample_metadata[folder_name]
            if folder_name.find(k) != -1:
                return self.sample_metadata[folder_name]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, folder_name))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            row_meta = {}
            for field in row._fields:
                if field != 'folder_name':
                    row_meta[field] = getattr(row, field)
            if row.folder_name:
                assert(row.folder_name not in sample_metadata)
                sample_metadata[row.folder_name] = row_meta
        return sample_metadata

    def _read_metadata(self, metadata_path):
        # regexps are to cope with slightly different formatting between the two tabs
        # we also have a mix of pool and bpa-id metadata
        field_spec = [
            ('dataset_id', 'data set id'),
            ('total_samples', 'total samples'),
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work order'),
            ('omics_type', 'omics type'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops'),
            ('folder_name', re.compile(r'^[Ff]ile name.*')),
            ('facility_project_code__facility_experiment_code', re.compile(r'^facility.project.code.*')),
            ('species', re.compile(r'^species.*')),
            ('other_protein_sequences', re.compile(r'^other protein sequences.*')),
            ('tissue', re.compile('^tissue.*')),
            ('cell_type', re.compile(r'^cell type.*')),
            ('labelling', re.compile(r'^labelling.*')),
            ('growth_protocol', re.compile(r'^growth protocol.*')),
            ('treatment_protocol', re.compile(r'^treatment protocol.*')),
            ('extract_protocol', 'extract protocol'),
            ('data_analysis_date', re.compile(r'^(date analysis date yyyy-mm-dd|date)$'), ingest_utils.get_date_isoformat),
            ('version_genome_or_database', 'version (genome or database)'),
            ('translation_3_frame_or_6_frame', 'translation (3 frame or 6 frame)'),
            ('proteome_size', 'proteome size'),
        ]
        rows = []
        for sheet_name in self.sheet_names:
            wrapper = ExcelWrapper(
                field_spec,
                metadata_path,
                sheet_name=sheet_name,
                header_length=2,
                column_name_row_index=1,
                formatting_info=True)
            rows += list(wrapper.get_all())
        # there are a bunch of duplicate rows, because per-file metadata is included
        # in the source spreadsheet
        #
        # we don't need that, so we simply don't include the per-file metadata in
        # `field_spec`, which means we can discard the duplicates here
        return list(set(rows))


class StemcellsMetabolomicsAnalysedContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-06-07/']
    name = 'stemcell-metabolomics-analysed'
    sheet_name = 'Metabolomics-Analysed'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpaops):
        bpaops = bpaops_clean(bpaops)
        if bpaops in self.sample_metadata:
            return self.sample_metadata[bpaops]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, bpaops))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            row_meta = {}
            for field in row._fields:
                if field != 'bpaops':
                    row_meta[field] = getattr(row, field)
            if row.bpaops:
                assert(row.bpaops not in sample_metadata)
                sample_metadata[row.bpaops] = row_meta
        return sample_metadata

    def _read_metadata(self, metadata_path):
        # regexps are to cope with slightly different formatting between the two tabs
        # we also have a mix of pool and bpa-id metadata
        field_spec = [
            ('dataset_id', 'data set id'),
            ('total_samples', 'total samples'),
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work order'),
            ('omics_type', 'omics type'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops', bpaops_clean),
            ('tissue', re.compile('^tissue.*')),
            ('cell_type', re.compile(r'^cell type.*')),
            ('growth_protocol', re.compile(r'^growth protocol.*')),
            ('extract_protocol', 'extract protocol'),
            ('data_analysis_date', 'data analysis date', ingest_utils.get_date_isoformat),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        # there are a bunch of duplicate rows, because per-file metadata is included
        # in the source spreadsheet
        #
        # we don't need that, so we simply don't include the per-file metadata in
        # `field_spec`, which means we can discard the duplicates here
        return list(set(wrapper.get_all()))
