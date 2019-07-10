import re
from glob import glob
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper, make_field_definition as fld
from ...util import make_logger, one

logger = make_logger(__name__)


def date_or_str(v):
    d = ingest_utils.get_date_isoformat(v, silent=True)
    if d is not None:
        return d
    return v


class GAPSampleContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/plants_staging/metadata/2019-07-10/']
    metadata_patterns = [re.compile(r'^.*\.xlsx$')]
    name = 'gap-sample-contextual'

    def __init__(self, path):
        self.sample_metadata = self._read_metadata(one(glob(path + '/*.xlsx')))

    def get(self, sample_id, bpa_library_id):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(sample_id)))
        return {}

    def _read_metadata(self, fname):

        field_spec = [
            fld('sample_submitter_name', 'sample_submitter_name'),
            fld('sample_submission_date', 'sample_submission_date', coerce=ingest_utils.get_date_isoformat),
            fld('sample_id', 'sample_id', coerce=ingest_utils.extract_ands_id),
            fld('library_id', 'library_id', coerce=ingest_utils.extract_ands_id),
            fld('dataset_id', 'dataset_id', coerce=ingest_utils.extract_ands_id),
            fld('scientific_name', 'scientific_name'),
            fld('living_collections_catalog_number', 'living_collections_catalog_number'),
            fld('living_collections_record_number', 'living_collections_record_number'),
            fld('living_collections_recorded_by', 'living_collections_recorded_by'),
            fld('living_collections_event_date', 'living_collections_event_date', coerce=ingest_utils.get_date_isoformat),
            fld('voucher_catalog_number', 'voucher_catalog_number'),
            fld('voucher_record_number', 'voucher_record_number'),
            fld('voucher_recorded_by', 'voucher_recorded_by'),
            fld('voucher_event_date', 'voucher_event_date', coerce=ingest_utils.get_date_isoformat),
            fld('preservation_type', 'preservation_type'),
            fld('preservation_temperature', 'preservation_temperature'),
            fld('preservation_date_begin', 'preservation_date_begin', coerce=ingest_utils.get_date_isoformat),
            fld('genomic_material_associated_references', 'genomic_material_associated_references'),
            fld('genomic_material_preparation_type', 'genomic_material_preparation_type'),
            fld('genomic_material_preparation_process', 'genomic_material_preparation_process'),
            fld('genomic_material_preparation_materials', 'genomic_material_preparation_materials'),
            fld('genomic_material_prepared_by', 'genomic_material_prepared_by'),
            fld('genomic_material_preparation_date', 'genomic_material_preparation_date', coerce=ingest_utils.get_date_isoformat),
]

        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=1,
            column_name_row_index=0,
            suggest_template=True)
        for error in wrapper.get_errors():
            logger.error(error)

        name_mapping = {
            'decimal_longitude': 'longitude',
            'decimal_latitude': 'latitude',
            'klass': 'class',
        }

        sample_metadata = {}
        for row in wrapper.get_all():
            if not row.sample_id:
                continue
            assert(row.sample_id not in sample_metadata)
            sample_id = ingest_utils.extract_ands_id(row.sample_id)
            sample_metadata[sample_id] = row_meta = {}
            for field in row._fields:
                value = getattr(row, field)
                if field == 'sample_id':
                    continue
                row_meta[name_mapping.get(field, field)] = value
        return sample_metadata

