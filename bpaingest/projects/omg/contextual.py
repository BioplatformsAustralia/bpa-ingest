import re
from glob import glob
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper
from ...util import make_logger, strip_to_ascii, one

logger = make_logger(__name__)


class OMGSampleContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/omg_staging/metadata/2017-05-17/']
    metadata_patterns = [re.compile(r'^.*\.xlsx$')]
    name = 'omg-sample-contextual'

    def __init__(self, path):
        self.sample_metadata = self._read_metadata(one(glob(path + '/*.xlsx')))

    def get(self, bpa_id):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(bpa_id)))
        return {}

    def _read_metadata(self, fname):
        field_spec = [
            ('bpa_id', 'bpa_id', ingest_utils.extract_bpa_id),
            ('voucher_id', 'voucher_id'),
            ('tissue_number', 'tissue_number'),
            ('institution_name', 'institution_name'),
            ('tissue_collection', 'tissue_ collection'),
            ('custodian', 'custodian'),
            ('access_rights', 'access_rights'),
            ('tissue_type', 'tissue_type'),
            ('tissue_preservation', 'tissue_preservation'),
            ('sample_quality', 'sample_quality'),
            ('taxon_id', 'taxon_id', ingest_utils.get_int),
            ('phylum', 'phylum'),
            ('class_', 'class'),
            ('order', 'order'),
            ('family', 'family'),
            ('genus', 'genus'),
            ('species', 'species'),
            ('subspecies', 'subspecies'),
            ('common_name', 'common_name'),
            ('identified_by', 'identified_by'),
            ('collection_date', 'collection_date', ingest_utils.get_date_isoformat),
            ('collector', 'collector'),
            ('collection_method', 'collection_method'),
            ('collector_sample_id', 'collector_sample_id'),
            ('wild_captive', 'wild_captive'),
            ('source_population', 'source_population'),
            ('country', 'country'),
            ('state_or_region', 'state_or_region'),
            ('location_text', 'location_text'),
            ('habitat', 'habitat'),
            ('decimal_latitude', 'decimal_latitude'),
            ('decimal_longitude', 'decimal_longitude'),
            ('coord_uncertainty_metres', 'coord_uncertainty_metres'),
            ('sex', 'sex'),
            ('life_stage', 'life-stage'),
            ('birth_date', 'birth_date', ingest_utils.get_date_isoformat),
            ('death_date', 'death_date', ingest_utils.get_date_isoformat),
            ('associated_media', 'associated_media'),
            ('ancilliary_notes', 'ancilliary_notes'),
            ('barcode_id', 'barcode_id'),
            ('ala_specimen_url', 'ala_specimen_url'),
            ('prior_genetics', 'prior_genetics'),
            ('dna_extraction_date', 'dna_extraction_date', ingest_utils.get_date_isoformat),
            ('dna_extracted_by', 'dna_extracted_by'),
            ('dna_extraction_method', 'dna_extraction_method'),
            ('dna_conc_ng_ul', 'dna_conc_ng_ul'),
        ]

        wrapper = ExcelWrapper(
            field_spec,
            fname,
            sheet_name=None,
            header_length=1,
            column_name_row_index=0,
            formatting_info=True)
        name_mapping = {
            'decimal_longitude': 'longitude',
            'decimal_latitude': 'latitude',
            'class_': 'class',
        }
        sample_metadata = {}
        for row in wrapper.get_all():
            if not row.bpa_id:
                continue
            assert(row.bpa_id not in sample_metadata)
            bpa_id = ingest_utils.extract_bpa_id(row.bpa_id)
            sample_metadata[bpa_id] = row_meta = {}
            for field in row._fields:
                value = getattr(row, field)
                if field == 'bpa_id':
                    continue
                row_meta[name_mapping.get(field, field)] = value
        return sample_metadata
