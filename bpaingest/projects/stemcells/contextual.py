from ...util import make_logger, one
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper
from glob import glob


logger = make_logger(__name__)


class StemcellAGRFTranscriptomeContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/current/']
    name = 'stemcell-agrf-transcriptome'

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
            ('data_set_id', 'data set id'),
            ('bpa_id', 'bpa identifier', ingest_utils.extract_bpa_id),
            ('number_samples', 'number samples'),
            ('submitter', 'submitter'),
            ('group', 'group group'),
            ('stem_cell_type', 'stem cell type'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work order'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated'),
            ('archive_ingestion_date', 'archive ingestion date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops'),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('data_set', 'data set'),
            ('species', 'species'),
            ('sample_description', 'sample description'),
            ('tissue', 'tissue'),
            ('cell_type', 'celll type'),
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
            sheet_name='AGRF-Transcriptome',
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        return wrapper.get_all()


class StemcellAGRFsmRNAContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/current/']
    name = 'stemcell-agrf-smallrna'

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
            ('data_set_id', 'data set id'),
            ('bpa_id', 'bpa identifier', ingest_utils.extract_bpa_id),
            ('number_samples', 'number samples'),
            ('submitter', 'submitter'),
            ('group', 'research group'),
            ('stem_cell_type', 'stem cell type'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work order'),
            ('analytical_platform', 'analytical platform'),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated'),
            ('archive_ingestion_date', 'archive ingestion date  yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops'),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('data_set', 'data set'),
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
            sheet_name='AGRF-smRNA',
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        return wrapper.get_all()


class StemcellRamaciottiSingleCell(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/current/']
    name = 'stemcell-ramaciotti-singlecell'

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
            ('data_set_id', 'data set id'),
            ('bpa_id', 'bpa identifier', ingest_utils.extract_bpa_id),
            ('number_samples', 'number samples'),
            ('submitter', 'submitter'),
            ('group', 'research group'),
            ('stem_cell_type', 'stem cell type'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work oder'),
            ('data_set', 'data set'),
            ('analytical_platform', 'analitical platfom'),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submision date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('sample_submission_date', 'sample submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated', ingest_utils.get_date_isoformat),
            ('archive_ingestion_date', 'archive ingestion date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('species', 'species'),
            ('sample_description', 'sample description'),
            ('tissue', 'tissue'),
            ('cell_type', 'celll type'),
            ('disease_state', 'disease state'),
            ('growth_protocol', 'growth protocol'),
            ('treatment_protocol', 'treatment protocol'),
            ('extract_protocol', 'extract protocol'),
            ('library_strategy', 'library strategy'),
            ('insert_size_range', 'insert size range'),
            ('library_construction_protocol', 'library construction protocol'),
            ('platform', 'platform')]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name='Ramaciotti-Single Cell ',
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        return wrapper.get_all()


class StemcellMetabolomicsContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/stemcell/projectdata/current/']
    name = 'stemcell-metabolomics'

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
            ('data_set_id', 'data set id'),
            ('bpa_id', 'bpa identifier', ingest_utils.extract_bpa_id),
            ('submitter', 'submitter'),
            ('group', 'research group'),
            ('stem_cell_type', 'stem cell type'),
            ('stem_cell_state', 'stem cell state'),
            ('work_order', 'work oder'),
            ('analytical_platform', 'analytical platform', fix_analytical_platform),
            ('facility', 'facility'),
            ('contextual_data_submission_date', 'contextual data submission date yyyy-mm-dd', ingest_utils.get_date_isoformat),
            ('date_submission', 'date submission', ingest_utils.get_date_isoformat),
            ('data_generated', 'data generated'),
            ('archive_ingestion_date', 'archive ingestion date', ingest_utils.get_date_isoformat),
            ('bpaops', 'bpaops'),
            ('sample_name', 'sample name'),
            ('replicate_group_id', 'replicate group id'),
            ('data_set', 'data set'),
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
            sheet_name='Metabolomics_Raw',
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        return wrapper.get_all()
