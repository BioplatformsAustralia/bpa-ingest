from ...util import make_logger, one
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper, make_field_definition as fld
from .util import fix_analytical_platform
from glob import glob


def bpaops_clean(s):
    return s.lower().replace('-', '')


logger = make_logger(__name__)


class StemcellsTranscriptomeContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-28/']
    name = 'stemcell-agrf-transcriptome'
    sheet_name = 'Transcriptome'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, sample_id):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, sample_id))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.sample_id is None:
                continue
            assert(row.sample_id not in sample_metadata)
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                if field != 'sample_id':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld('submitter', 'submitter'),
            fld('research_group', 'research group'),
            fld('stem_cell_line', 'stem cell line'),
            fld('stem_cell_state', 'stem cell state'),
            fld('contextual_data_submission_date', 'contextual data submission date', coerce=ingest_utils.get_date_isoformat),
            fld('sample_submission_date', 'sample submission date', coerce=ingest_utils.get_date_isoformat),
            fld('archive_ingestion_date', 'archive ingestion date', coerce=ingest_utils.get_date_isoformat),
            fld('total_samples', 'total samples', coerce=ingest_utils.get_int),
            fld('bpa_dataset_id', 'data set id', coerce=ingest_utils.extract_ands_id),
            fld('sample_id', 'bpa id', coerce=ingest_utils.extract_ands_id),
            fld('plate_number', 'plate number'),
            fld('well_number', 'well number', coerce=ingest_utils.get_int),
            fld('sample_name', 'sample name'),
            fld('replicate_group_id', 'replicate group id'),
            fld('omics', 'omics'),
            fld('analytical_platform', 'analytical platform', coerce=fix_analytical_platform),
            fld('facility', 'facility'),
            fld('species', 'species'),
            fld('sample_description', 'sample description'),
            fld('tissue', 'tissue'),
            fld('cell_type', 'cell type'),
            fld('disease_state', 'disease state'),
            fld('labelling', 'labelling'),
            fld('organism_part', 'organism part'),
            fld('growth_protocol', 'growth protocol'),
            fld('treatment_protocol', 'treatment protocol'),
            fld('extract_protocol', 'extract protocol'),
            fld('library_strategy', 'library strategy'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=3,
            column_name_row_index=2)
        for error in wrapper.get_errors():
            logger.error(error)
        return wrapper.get_all()


class StemcellsSmallRNAContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-28/']
    name = 'stemcell-agrf-smallrna'
    sheet_name = 'Small RNA'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, sample_id):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, sample_id))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.sample_id is None:
                continue
            assert(row.sample_id not in sample_metadata)
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                if field != 'sample_id':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld('submitter', 'submitter'),
            fld('research_group', 'research group'),
            fld('stem_cell_line', 'stem cell line'),
            fld('stem_cell_state', 'stem cell state'),
            fld('contextual_data_submission_date', 'contextual data submission date', coerce=ingest_utils.get_date_isoformat),
            fld('sample_submission_date', 'sample submission date', coerce=ingest_utils.get_date_isoformat),
            fld('archive_ingestion_date', 'archive ingestion date', coerce=ingest_utils.get_date_isoformat),
            fld('total_samples', 'total samples', coerce=ingest_utils.get_int),
            fld('bpa_dataset_id', 'data set id', coerce=ingest_utils.extract_ands_id),
            fld('sample_id', 'bpa id', coerce=ingest_utils.extract_ands_id),
            fld('plate_number', 'plate number', coerce=ingest_utils.get_int),
            fld('well_number', 'well number', coerce=ingest_utils.get_int),
            fld('sample_name', 'sample name'),
            fld('replicate_group_id', 'replicate group id'),
            fld('omics', 'omics'),
            fld('analytical_platform', 'analytical platform', coerce=fix_analytical_platform),
            fld('facility', 'facility'),
            fld('species', 'species'),
            fld('sample_description', 'sample description'),
            fld('tissue', 'tissue'),
            fld('cell_type', 'cell type'),
            fld('disease_state', 'disease state'),
            fld('labelling', 'labelling'),
            fld('organism_part', 'organism part'),
            fld('growth_protocol', 'growth protocol'),
            fld('treatment_protocol', 'treatment protocol'),
            fld('extract_protocol', 'extract protocol'),
            fld('library_strategy', 'library strategy'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=3,
            column_name_row_index=2)
        for error in wrapper.get_errors():
            logger.error(error)
        return wrapper.get_all()


class StemcellsSingleCellRNASeq(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-28/']
    name = 'stemcell-ramaciotti-singlecell'
    sheet_name = 'Single Cell RNAseq'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, sample_id):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, sample_id))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.sample_id is None:
                continue
            assert(row.sample_id not in sample_metadata)
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                if field != 'sample_id':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld('submitter', 'submitter'),
            fld('research_group', 'research group'),
            fld('stem_cell_line', 'stem cell line'),
            fld('stem_cell_state', 'stem cell state'),
            fld('contextual_data_submission_date', 'contextual data submission date', coerce=ingest_utils.get_date_isoformat),
            fld('sample_submission_date', 'sample submission date', coerce=ingest_utils.get_date_isoformat),
            fld('archive_ingestion_date', 'archive ingestion date', coerce=ingest_utils.get_date_isoformat),
            fld('total_samples', 'total samples', coerce=ingest_utils.get_int),
            fld('bpa_dataset_id', 'data set id', coerce=ingest_utils.extract_ands_id),
            fld('sample_id', 'bpa id', coerce=ingest_utils.extract_ands_id),
            fld('plate_number', 'plate number', coerce=ingest_utils.get_int),
            fld('well_number', 'well number', coerce=ingest_utils.get_int),
            fld('sample_name', 'sample name'),
            fld('replicate_group_id', 'replicate group id'),
            fld('omics', 'omics'),
            fld('analytical_platform', 'analytical platform', coerce=fix_analytical_platform),
            fld('facility', 'facility'),
            fld('species', 'species'),
            fld('sample_description', 'sample description'),
            fld('tissue', 'tissue'),
            fld('cell_type', 'cell type'),
            fld('disease_state', 'disease state'),
            fld('labelling', 'labelling'),
            fld('organism_part', 'organism part'),
            fld('growth_protocol', 'growth protocol'),
            fld('treatment_protocol', 'treatment protocol'),
            fld('extract_protocol', 'extract protocol'),
            fld('library_strategy', 'library strategy'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=3,
            column_name_row_index=2)
        for error in wrapper.get_errors():
            logger.error(error)
        return wrapper.get_all()


class StemcellsMetabolomicsContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-28/']
    name = 'stemcell-metabolomics'
    sheet_name = 'Metabolomics'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, sample_id, analytical_platform):
        tpl = (sample_id, analytical_platform)
        if tpl in self.sample_metadata:
            return self.sample_metadata[tpl]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, tpl))
        logger.warning(list(sorted(self.sample_metadata.keys())))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.sample_id is None:
                continue
            tpl = (row.sample_id, row.analytical_platform)
            assert(tpl not in sample_metadata)
            sample_metadata[tpl] = row_meta = {}
            for field in row._fields:
                if field != 'sample_id' and field != 'analytical_platform':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld('submitter', 'submitter'),
            fld('research_group', 'research group'),
            fld('stem_cell_line', 'stem cell line'),
            fld('stem_cell_state', 'stem cell state'),
            fld('contextual_data_submission_date', 'contextual data submission date', coerce=ingest_utils.get_date_isoformat),
            fld('sample_submission_date', 'sample submission date', coerce=ingest_utils.get_date_isoformat),
            fld('archive_ingestion_date', 'archive ingestion date', coerce=ingest_utils.get_date_isoformat),
            fld('total_samples', 'total samples', coerce=ingest_utils.get_int),
            fld('bpa_dataset_id', 'data set id', coerce=ingest_utils.extract_ands_id),
            fld('sample_id', 'bpa id', coerce=ingest_utils.extract_ands_id),
            fld('plate_number', 'plate number', coerce=ingest_utils.get_int),
            fld('well_number', 'well number', coerce=ingest_utils.get_int),
            fld('sample_name', 'sample name', coerce=ingest_utils.get_int),
            fld('replicate_group_id', 'replicate group id'),
            fld('omics', 'omics'),
            fld('analytical_platform', 'analytical platform', coerce=fix_analytical_platform),
            fld('facility', 'facility'),
            fld('species', 'species'),
            fld('sample_description', 'sample_description'),
            fld('tissue', 'tissue'),
            fld('cell_type', 'cell type'),
            fld('disease_state', 'disease state'),
            fld('organism_part', 'organism part'),
            fld('growth_protocol', 'growth protocol'),
            fld('extract_protocol', 'extract protocol'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=self.sheet_name,
            header_length=3,
            column_name_row_index=2)
        for error in wrapper.get_errors():
            logger.error(error)
        return wrapper.get_all()


class StemcellsProteomicsContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/2017-07-28/']
    name = 'stemcell-proteomics'
    sheet_names = ['Proteomics']

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, sample_id):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, sample_id))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            row_meta = {}
            for field in row._fields:
                if field != 'sample_id':
                    row_meta[field] = getattr(row, field)
            if row.sample_id:
                assert(row.sample_id not in sample_metadata)
                sample_metadata[row.sample_id] = row_meta
        return sample_metadata

    def _read_metadata(self, metadata_path):
        # regexps are to cope with slightly different formatting between the two tabs
        # we also have a mix of pool and bpa-id metadata
        field_spec = [
            fld('submitter', 'submitter'),
            fld('research_group', 'research group'),
            fld('stem_cell_line', 'stem cell line'),
            fld('stem_cell_state', 'stem cell state'),
            fld('contextual_data_submission_date', 'contextual data submission date', coerce=ingest_utils.get_date_isoformat),
            fld('sample_submission_date', 'sample submission date', coerce=ingest_utils.get_date_isoformat),
            fld('archive_ingestion_date', 'archive ingestion date', coerce=ingest_utils.get_date_isoformat),
            fld('total_samples', 'total samples', coerce=ingest_utils.get_int),
            fld('bpa_dataset_id', 'dataset id', coerce=ingest_utils.extract_ands_id),
            fld('sample_id', 'bpa id', coerce=ingest_utils.extract_ands_id),
            fld('plate_number', 'plate number', coerce=ingest_utils.get_int),
            fld('well_number', 'well number', coerce=ingest_utils.get_int),
            fld('sample_name', 'sample name', coerce=ingest_utils.get_int),
            fld('replicate_group_id', 'replicate group id'),
            fld('omics', 'omics'),
            fld('analytical_platform', 'analytical platform', coerce=fix_analytical_platform),
            fld('facility', 'facility'),
            fld('species', 'species'),
            fld('sample_description', 'sample_description'),
            fld('sample_type', 'sample type'),
            fld('passage_number', 'passage number'),
            fld('other_protein_sequences', 'other protein sequences'),
            fld('tissue', 'tissue'),
            fld('cell_type', 'cell type'),
            fld('disease_state', 'disease state'),
            fld('labelling', 'labelling'),
            fld('growth_protocol', 'growth protocol'),
            fld('treatment_protocol', 'treatment protocol'),
            fld('extract_protocol', 'extract protocol'),
        ]
        rows = []
        for sheet_name in self.sheet_names:
            wrapper = ExcelWrapper(
                field_spec,
                metadata_path,
                sheet_name=sheet_name,
                header_length=3,
                column_name_row_index=2)
            for error in wrapper.get_errors():
                logger.error(error)
            rows += list(wrapper.get_all())
        # there are a bunch of duplicate rows, because per-file metadata is included
        # in the source spreadsheet
        #
        # we don't need that, so we simply don't include the per-file metadata in
        # `field_spec`, which means we can discard the duplicates here
        return list(set(rows))
