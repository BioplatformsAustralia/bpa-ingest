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
            ('bpa_id', 'bpa identifier', ingest_utils.extract_bpa_id),
            ('sc_samples', 'sc samples #', None),
            ('number_samples', 'number samples', None),
            ('submitter', 'submitter', None),
            ('group', 'group', None),
            ('research', 'research', None),
            ('stem_cell_research', 'stem cell research', None),
            ('data_set', 'data set', None, None),
            ('sample_name', 'sample name', None),
            ('replicate_group_id', 'replicate group id', None),
            ('species', 'species', None),
            ('description', 'sample description', None),
            ('tissue', 'tissue', None),
            ('cell_type', 'celll type', None),
            ('disease_state', 'disease state', None),
            ('growth_protocol', 'growth protocol', None),
            ('treatment_protocol', 'treatment protocol', None),
            ('extract_protocol', 'extract protocol', None),
            ('library_strategy', 'library strategy', None),
            ('insert_size_range', 'insert size range', None),
            ('library_construction_process', 'library construction protocol', None),
            ('analytical_platform', 'platform', None),
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
            ('bpa_id', 'bpa identifier', ingest_utils.extract_bpa_id),
            ('sc_samples', 'sc samples #', None),
            ('number_samples', 'number samples', None),
            ('submitter', 'submitter', None),
            ('group', 'group', None),
            ('research', 'research', None),
            ('stem_cell_research', 'stem cell research', None),
            ('data_set', 'data set', None, None),
            ('sample_name', 'sample name', None),
            ('replicate_group_id', 'replicate group id', None),
            ('species', 'species', None),
            ('description', 'sample description', None),
            ('tissue', 'tissue', None),
            ('cell_type', 'celll type', None),
            ('disease_state', 'disease state', None),
            ('growth_protocol', 'growth protocol', None),
            ('treatment_protocol', 'treatment protocol', None),
            ('extract_protocol', 'extract protocol', None),
            ('library_strategy', 'library strategy', None),
            ('insert_size_range', 'insert size range', None),
            ('library_construction_process', 'library construction protocol', None),
            ('analytical_platform', 'platform', None),
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
            ('bpa_id', 'bpa identifier', ingest_utils.extract_bpa_id),
            ('sc_samples', 'sc samples #', None),
            ('number_samples', 'number samples', None),
            ('submitter', 'submitter', None),
            ('group', 'group', None),
            ('research', 'research', None),
            ('stem_cell_research', 'stem cell research', None),
            ('data_set', 'data set', None, None),
            ('sample_name', 'sample name', None),
            ('replicate_group_id', 'replicate group id', None),
            ('species', 'species', None),
            ('description', 'sample description', None),
            ('tissue', 'tissue', None),
            ('cell_type', 'celll type', None),
            ('disease_state', 'disease state', None),
            ('growth_protocol', 'growth protocol', None),
            ('treatment_protocol', 'treatment protocol', None),
            ('extract_protocol', 'extract protocol', None),
            ('library_strategy', 'library strategy', None),
            ('insert_size_range', 'insert size range', None),
            ('library_construction_process', 'library construction protocol', None),
            ('analytical_platform', 'platform', None),
        ]
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
        if bpa_id in self.sample_metadata:
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
        field_spec = [
            ('bpa_id', 'bpa identifier', ingest_utils.extract_bpa_id),
            ('sc_samples', 'sc samples #', None),
            ('number_samples', 'number samples', None),
            ('submitter', 'submitter', None),
            ('group', 'group', None),
            ('research', 'research', None),
            ('stem_cell_research', 'stem cell research', None),
            ('data_set', 'data set', None, None),
            ('sample_name', 'sample name', None),
            ('replicate_group_id', 'replicate group id', None),
            ('species', 'species', None),
            ('description', 'sample description', None),
            ('tissue', 'tissue', None),
            ('cell_type', 'celll type', None),
            ('disease_state', 'disease state', None),
            ('growth_protocol', 'growth protocol', None),
            ('treatment_protocol', 'treatment protocol', None),
            ('extract_protocol', 'extract protocol', None),
            ('library_strategy', 'library strategy', None),
            ('insert_size_range', 'insert size range', None),
            ('library_construction_process', 'library construction protocol', None),
            ('analytical_platform', 'platform', None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name='Metabolomics_Raw',
            header_length=2,
            column_name_row_index=1,
            formatting_info=True)
        return wrapper.get_all()