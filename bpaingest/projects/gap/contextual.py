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


class GAPLibraryContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/plants_staging/metadata/2020-02-14/']
    metadata_patterns = [re.compile(r'^.*\.xlsx$')]
    name = 'gap-library-contextual'

    def __init__(self, path):
        self.library_metadata = self._read_metadata(one(glob(path + '/*.xlsx')))

    def get(self, library_id):
        if library_id in self.library_metadata:
            return self.library_metadata[library_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(library_id)))
        return {}

    def _read_metadata(self, fname):

        field_spec = [
            fld('data_type', 'data_type'),
            fld('project_aim', 'project_aim'),
            fld('sample_submitter_name', 'sample_submitter_name'),
            fld('sample_submitter_email', 'sample_submitter_email'),
            fld('sample_submission_date', 'sample_submission_date', coerce=ingest_utils.get_date_isoformat),
            fld('sample_id', 'bioplatforms_sample_id', coerce=ingest_utils.extract_ands_id),
            fld('library_id', 'bioplatforms_library_id', coerce=ingest_utils.extract_ands_id),
            fld('dataset_id', 'bioplatforms_dataset_id', coerce=ingest_utils.extract_ands_id),
            fld('nagoya_protocol_compliance', 'nagoya_protocol_compliance'),
            fld('nagoya_protocol_permit_number', 'nagoya_protocol_permit_number'),
            fld('scientific_name', 'scientific_name'),
            fld('scientific_name_authorship', 'scientific_name_authorship'),
            fld('family', 'family'),
            fld('id_vetting_by', 'id_vetting_by'),
            fld('bait_set_name', 'bait_set_name'),
            fld('bait_set_reference', 'bait_set_reference'),
            fld('living_collections_catalog_number', 'living _collections_catalog_number'),
            fld('living_collections_record_number', 'living _collections_record_number'),
            fld('living_collections_recorded_by', 'living _collections_recorded_by'),
            fld('living_collections_event_date', 'living _collections_event_date', coerce=ingest_utils.get_date_isoformat),
            fld('herbarium_code', 'herbarium_code'),
            fld('voucher_herbarium_collector_id', 'voucher_herbarium_collector_id'),
            fld('voucher_herbarium_catalog_number', 'voucher_herbarium_catalog_number'),
            fld('voucher_herbarium_record_number', 'voucher_herbarium_record_number'),
            fld('voucher_herbarium_recorded_by', 'voucher_herbarium_recorded_by'),
            fld('voucher_herbarium_event_date', 'voucher_herbarium_event_date', coerce=ingest_utils.get_date_isoformat),
            fld('silica_gel', 'silica_gel'),
            fld('silica_gel_pressed_sheet', 'silica_gel_pressed_sheet'),
            fld('dna_extract', 'dna_extract'),
            fld('dna_extract_pressed_sheet', 'dna_extract_pressed_sheet'),
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

        library_metadata = {}
        for row in wrapper.get_all():
            if not row.library_id:
                continue
            if row.library_id in library_metadata:
                raise Exception("duplicate library id: {}".format(row.library_id))
            library_id = ingest_utils.extract_ands_id(row.library_id)
            library_metadata[library_id] = row_meta = {}
            for field in row._fields:
                value = getattr(row, field)
                if field == 'library_id':
                    continue
                row_meta[name_mapping.get(field, field)] = value
        return library_metadata
