import re
from glob import glob
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper, make_field_definition as fld
from ...util import one
from ...abstract import BaseDatasetControlContextual


class OMGSampleContextual:
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/metadata/2022-12-02/"
    ]
    metadata_patterns = [re.compile(r"^OMG_samples_metadata.*\.xlsx$")]
    name = "omg-sample-contextual"

    def __init__(self, logger, path):
        self._logger = logger
        self.sample_metadata = self._read_metadata(one(glob(path + "/*.xlsx")))

    # The callee calls with (sample_id and library_id) as workaround to not knowing whether calling
    # this class method or the other library context class
    def get(self, bpa_sample_id, bpa_library_id):
        if bpa_sample_id in self.sample_metadata:
            return self.sample_metadata[bpa_sample_id]
        self._logger.warning(
            "no %s metadata available for: %s"
            % (type(self).__name__, repr(bpa_sample_id))
        )
        return {}

    def _read_metadata(self, fname):
        field_spec = [
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("voucher_number", "voucher_number", coerce=ingest_utils.int_or_comment),
            fld(
                "voucher_or_tissue_number",
                "voucher_or_tissue_number",
                coerce=ingest_utils.int_or_comment,
            ),
            fld("tissue_number", "tissue_number"),
            fld("institution_name", "institution_name"),
            fld("tissue_collection", "collection"),
            fld("custodian", "custodian"),
            fld("access_rights", "access_rights"),
            fld("tissue_type", "tissue_type"),
            fld("tissue_preservation", "tissue_preservation"),
            fld("sample_quality", "sample_quality"),
            fld("taxon_id", "taxon_id", coerce=ingest_utils.get_int),
            fld("phylum", "phylum"),
            fld("klass", "class"),
            fld("order", "order"),
            fld("family", "family"),
            fld("genus", "genus"),
            fld("species", "species"),
            fld("common_name", "common_name"),
            fld("identified_by", "identified_by"),
            fld("collection_date", "collection_date", coerce=ingest_utils.date_or_str),
            fld("collector", "collector"),
            fld("collection_method", "collection_method"),
            fld("collector_sample_id", "collector_sample_id"),
            fld("wild_captive", "wild_captive"),
            fld("source_population", "source_population"),
            fld("country", "country"),
            fld("state_or_region", "state_or_region"),
            fld("location_text", "location_text"),
            fld("habitat", "habitat"),
            fld(
                "decimal_latitude",
                re.compile(r"^decimal_latitude.*$"),
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "decimal_longitude",
                re.compile(r"^decimal_longitude.*$"),
                coerce=ingest_utils.get_clean_number,
            ),
            fld("coord_uncertainty_metres", "coord_uncertainty_metres"),
            fld("sex", "sex"),
            fld("life_stage", "life-stage"),
            fld("birth_date", "birth_date", coerce=ingest_utils.date_or_str),
            fld("death_date", "death_date", coerce=ingest_utils.date_or_str),
            fld("associated_media", "associated_media"),
            fld("ancillary_notes", "ancillary_notes"),
            fld("barcode_id", "barcode_id"),
            fld("ala_specimen_url", "ala_specimen_url"),
            fld("prior_genetics", "prior_genetics"),
            fld(
                "dna_extraction_date",
                "dna_extraction_date",
                coerce=ingest_utils.date_or_str,
            ),
            fld("dna_extracted_by", "dna_extracted_by"),
            fld("dna_extraction_method", "dna_extraction_method"),
            fld("dna_conc_ng_ul", "dna_conc_ng_ul"),
            fld("taxonomic_group", "taxonomic_group"),
            fld("trace_lab", "trace_lab"),
            fld("type_status", "type_status"),
            fld("ddrad_data", "ddrad_data"),
            fld("ddrad_dataset_ids", "ddrad_dataset_ids"),
            fld("exon_capture_data", "exon_capture_data"),
            fld("exon_capture_dataset_ids", "exon_capture_dataset_ids"),
            fld("genome_data", "genome_data"),
            fld("genome_dataset_ids", "genome_dataset_ids"),
            fld("subspecies_or_variant", "subspecies or variant"),
        ]

        wrapper = ExcelWrapper(
            self._logger,
            field_spec,
            fname,
            sheet_name=None,
            header_length=1,
            column_name_row_index=0,
            suggest_template=True,
        )
        for error in wrapper.get_errors():
            self._logger.error(error)

        name_mapping = {
            "decimal_longitude": "longitude",
            "decimal_latitude": "latitude",
            "klass": "class",
        }

        sample_metadata = {}
        for row in wrapper.get_all():
            if not row.bpa_sample_id:
                continue
            assert row.bpa_sample_id not in sample_metadata
            bpa_sample_id = ingest_utils.extract_ands_id(
                self._logger, row.bpa_sample_id
            )
            sample_metadata[bpa_sample_id] = row_meta = {}
            for field in row._fields:
                value = getattr(row, field)
                if field == "bpa_sample_id":
                    continue
                row_meta[name_mapping.get(field, field)] = value
        return sample_metadata


class OMGLibraryContextual:
    # this spreadsheet was only used for early data.
    # for more recent data, it is included in the transfer metadata
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/metadata/2020-12-22/"
    ]
    metadata_patterns = [re.compile(r"^OMG_library_metadata.*\.xlsx$")]
    name = "omg-library-contextual"

    def __init__(self, logger, path):
        self._logger = logger
        self.library_metadata = self._read_metadata(one(glob(path + "/*.xlsx")))

    # The callee calls with (sample_id and library_id) as workaround to not knowing whether calling
    # this class method or the other library context class
    def get(self, bpa_sample_id, bpa_library_id):
        if bpa_library_id in self.library_metadata:
            return self.library_metadata[bpa_library_id]
        return {}

    def _read_metadata(self, fname):
        field_spec = [
            fld(
                "bpa_library_id", "bpa_library_id", coerce=ingest_utils.extract_ands_id
            ),
            fld("bpa_sample_id", "bpa_sample_id", coerce=ingest_utils.extract_ands_id),
            fld("library_type", "library_type"),
            fld(
                "library_prep_date",
                "library_prep_date",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld("library_prepared_by", "library_prepared_by"),
            fld("library_prep_method", "library_prep_method"),
            fld("experimental_design", "experimental_design"),
            fld("omg_project", "omg_project"),
            fld("data_custodian", "data_custodian"),
            fld("dna_treatment", "dna_treatment"),
            fld("library_index_id", "library_index_id"),
            fld("library_index_sequence", "library_index_sequence"),
            fld("library_oligo_sequence", "library_oligo_sequence"),
            fld("library_pcr_reps", "library_pcr_reps"),
            fld("library_pcr_cycles", "library_pcr_cycles"),
            fld("library_ng_ul", "library_ng_ul"),
            fld("library_comments", "library_comments"),
            fld("library_location", "library_location"),
            fld("library_status", "library_status"),
        ]
        wrapper = ExcelWrapper(
            self._logger,
            field_spec,
            fname,
            sheet_name=None,
            header_length=1,
            column_name_row_index=0,
            suggest_template=True,
        )
        for error in wrapper.get_errors():
            self._logger.error(error)

        library_metadata = {}
        for row in wrapper.get_all():
            if not row.bpa_library_id:
                continue
            assert row.bpa_library_id not in library_metadata
            bpa_library_id = ingest_utils.extract_ands_id(
                self._logger, row.bpa_library_id
            )
            library_metadata[bpa_library_id] = row_meta = {}
            for field in row._fields:
                value = getattr(row, field)
                if field == "bpa_library_id" or field == "bpa_sample_id":
                    continue
                row_meta[field] = value
        return library_metadata


class OMGDatasetControlContextual(BaseDatasetControlContextual):
    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/omg_staging/dataset_control/2021-11-24/"
    ]
    name = "omg-dataset-contextual"
    contextual_linkage = ("bpa_sample_id", "bpa_library_id")
    additional_fields = [
        fld("bpa_dataset_id", "bpa_dataset_id"),
    ]
