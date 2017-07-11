from ...util import make_logger, one
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper
from glob import glob


def bpaops_clean(s):
    return s.lower().replace('-', '')


logger = make_logger(__name__)


class StemcellsTranscriptomeContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-07/']
    name = 'stemcell-agrf-transcriptome'
    sheet_name = 'Transcriptome'

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
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date', ingest_utils.get_date_isoformat),
            ('archive_ingestion_date', 'archive ingestion date', ingest_utils.get_date_isoformat),
            ('total_samples', 'total samples', ingest_utils.get_int),
            ('bpa_dataset_id', 'data set id', ingest_utils.extract_bpa_id),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id),
            ('plate_number', 'plate number'),
            ('well_number', 'well number', ingest_utils.get_int),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('omics', 'omics'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('species', 'species'),
            ('sample_description', 'sample description'),
            ('tissue', 'tissue'),
            ('cell_type', 'cell type'),
            ('disease_state', 'disease state'),
            ('labelling', 'labelling'),
            ('organism_part', 'organism part'),
            ('growth_protocol', 'growth protocol'),
            ('treatment_protocol', 'treatment protocol'),
            ('extract_protocol', 'extract protocol'),
            ('library_strategy', 'library strategy'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=3,
            column_name_row_index=2,
            formatting_info=True)
        return wrapper.get_all()


class StemcellsSmallRNAContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-07/']
    name = 'stemcell-agrf-smallrna'
    sheet_name = 'Small RNA'

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
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date', ingest_utils.get_date_isoformat),
            ('archive_ingestion_date', 'archive ingestion date', ingest_utils.get_date_isoformat),
            ('total_samples', 'total samples'),
            ('bpa_dataset_id', 'data set id', ingest_utils.extract_bpa_id),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id),
            ('plate_number', 'plate number', ingest_utils.get_int),
            ('well_number', 'well number', ingest_utils.get_int),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('omics', 'omics'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('species', 'species'),
            ('sample_description', 'sample description'),
            ('tissue', 'tissue'),
            ('cell_type', 'cell type'),
            ('disease_state', 'disease state'),
            ('labelling', 'labelling'),
            ('organism_part', 'organism part'),
            ('growth_protocol', 'growth protocol'),
            ('treatment_protocol', 'treatment protocol'),
            ('extract_protocol', 'extract protocol'),
            ('library_strategy', 'library strategy'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=3,
            column_name_row_index=2,
            formatting_info=True)
        return wrapper.get_all()


class StemcellsSingleCellRNASeq(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-07/']
    name = 'stemcell-ramaciotti-singlecell'
    sheet_name = 'Single Cell RNAseq'

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
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date', ingest_utils.get_date_isoformat),
            ('archive_ingestion_date', 'archive ingestion date', ingest_utils.get_date_isoformat),
            ('total_samples', 'total samples'),
            ('bpa_dataset_id', 'data set id', ingest_utils.extract_bpa_id),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id),
            ('plate_number', 'plate number', ingest_utils.get_int),
            ('well_number', 'well number', ingest_utils.get_int),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('omics', 'omics'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('species', 'species'),
            ('sample_description', 'sample description'),
            ('tissue', 'tissue'),
            ('cell_type', 'cell type'),
            ('disease_state', 'disease state'),
            ('labelling', 'labelling'),
            ('organism_part', 'organism part'),
            ('growth_protocol', 'growth protocol'),
            ('treatment_protocol', 'treatment protocol'),
            ('extract_protocol', 'extract protocol'),
            ('library_strategy', 'library strategy'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=3,
            column_name_row_index=2,
            formatting_info=True)
        return wrapper.get_all()


class StemcellsMetabolomicsContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-07/']
    name = 'stemcell-metabolomics'
    sheet_name = 'Metabolomics'

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
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date', ingest_utils.get_date_isoformat),
            ('archive_ingestion_date', 'archive ingestion date', ingest_utils.get_date_isoformat),
            ('total_samples', 'total samples', ingest_utils.get_int),
            ('bpa_dataset_id', 'data set id', ingest_utils.extract_bpa_id),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id),
            ('plate_number', 'plate number', ingest_utils.get_int),
            ('well_number', 'well number', ingest_utils.get_int),
            ('sample_name', 'sample name', ingest_utils.get_int),
            ('replicate_group_id', 'replicate group id'),
            ('omics', 'omics'),
            ('analytical_platform', 'analytical platform', fix_analytical_platform),
            ('facility', 'facility'),
            ('species', 'species'),
            ('sample_description', 'sample_description'),
            ('tissue', 'tissue'),
            ('cell_type', 'cell type'),
            ('disease_state', 'disease state'),
            ('organism_part', 'organism part'),
            ('growth_protocol', 'growth protocol'),
            ('extract_protocol', 'extract protocol'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=3,
            column_name_row_index=2,
            formatting_info=True)
        return wrapper.get_all()


class StemcellsProteomicsContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-07/']
    name = 'stemcell-proteomics'
    sheet_names = ['Proteomics']

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
            row_meta = {}
            for field in row._fields:
                if field != 'bpa_id':
                    row_meta[field] = getattr(row, field)
            if row.bpa_id:
                assert(row.bpa_id not in sample_metadata)
                sample_metadata[row.bpa_id] = row_meta
        return sample_metadata

    def _read_metadata(self, metadata_path):
        # regexps are to cope with slightly different formatting between the two tabs
        # we also have a mix of pool and bpa-id metadata
        field_spec = [
            ('submitter', 'submitter'),
            ('research_group', 'research group'),
            ('stem_cell_line', 'stem cell line'),
            ('stem_cell_state', 'stem cell state'),
            ('contextual_data_submission_date', 'contextual data submission date', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date', ingest_utils.get_date_isoformat),
            ('archive_ingestion_date', 'archive ingestion date', ingest_utils.get_date_isoformat),
            ('total_samples', 'total samples', ingest_utils.get_int),
            ('bpa_dataset_id', 'dataset id', ingest_utils.extract_bpa_id),
            ('bpa_id', 'bpa id', ingest_utils.extract_bpa_id),
            ('plate_number', 'plate number', ingest_utils.get_int),
            ('well_number', 'well number', ingest_utils.get_int),
            ('sample_name', 'sample name', ingest_utils.get_int),
            ('replicate_group_id', 'replicate group id'),
            ('omics', 'omics'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('species', 'species'),
            ('sample_description', 'sample_description'),
            ('sample_type', 'sample type'),
            ('passage_number', 'passage number'),
            ('other_protein_sequences', 'other protein sequences'),
            ('tissue', 'tissue'),
            ('cell_type', 'cell type'),
            ('disease_state', 'disease state'),
            ('labelling', 'labelling'),
            ('growth_protocol', 'growth protocol'),
            ('treatment_protocol', 'treatment protocol'),
            ('extract_protocol', 'extract protocol'),
        ]
        rows = []
        for sheet_name in self.sheet_names:
            wrapper = ExcelWrapper(
                field_spec,
                metadata_path,
                sheet_name=sheet_name,
                header_length=3,
                column_name_row_index=2,
                formatting_info=True)
            rows += list(wrapper.get_all())
        # there are a bunch of duplicate rows, because per-file metadata is included
        # in the source spreadsheet
        #
        # we don't need that, so we simply don't include the per-file metadata in
        # `field_spec`, which means we can discard the duplicates here
        return list(set(rows))
