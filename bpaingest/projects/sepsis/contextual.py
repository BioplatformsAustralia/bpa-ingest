from ...util import one
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper, make_field_definition as fld
from .strains import get_taxon_strain, map_taxon_strain_rows
from glob import glob


def date_or_comment(logger, val):
    # another mix of actual dates and free-text comments, clean up as much as we can
    # into standard dates, but if not we return the underlying value
    if val is None:
        return None
    val_as_date = ingest_utils.get_date_isoformat(logger, val)
    if val_as_date is not None:
        return val_as_date
    val = str(val).strip()
    if not val:
        return None
    return val


def get_gram_stain(logger, val):
    if val and val != "":
        val = val.lower()
        if "positive" in val:
            return "POS"
        elif "negative" in val:
            return "NEG"
    return None


def get_sex(logger, val):
    if val is None:
        return None
    val = val.lower()
    # order of these statements is significant
    if "female" in val:
        return "F"
    if "male" in val:
        return "M"
    if "ethics embargo" in val:
        return "ethics embargo"
    return None


def get_strain_or_isolate(logger, val):
    if val and val != "":
        # convert floats to str
        if isinstance(val, float):
            val = int(val)
        return str(val)
    return None


class SepsisBacterialContextual:
    """
    Bacterial sample metadata: used by each of the -omics classes below.
    """

    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/2019-06-13/bacterial/"
    ]
    name = "sepsis-bacterial"

    def __init__(self, logger, path):
        self._logger = logger
        xlsx_path = one(glob(path + "/*.xlsx"))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, sample_id, submission_obj):
        tpl = get_taxon_strain(submission_obj)
        if None in tpl:
            return {}
        # deliberate hard failure if the tuple isn't in our metadata: this simply shouldn't happen
        return self.sample_metadata[tpl]

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.taxon_or_organism is None or row.strain_or_isolate is None:
                continue
            strain_tuple = (row.taxon_or_organism, row.strain_or_isolate)
            assert strain_tuple not in sample_metadata
            sample_metadata[strain_tuple] = row_meta = {}
            for field in row._fields:
                if field != "taxon_or_organism" and field != "strain_or_isolate":
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld(
                "gram_stain",
                "Gram_staining_(positive_or_negative)",
                coerce=get_gram_stain,
            ),
            fld("taxon_or_organism", "Taxon_OR_organism"),
            fld("strain_or_isolate", "Strain_OR_isolate", coerce=get_strain_or_isolate),
            fld("serovar", "Serovar", coerce=ingest_utils.int_or_comment),
            fld("key_virulence_genes", "Key_virulence_genes"),
            fld("isolation_source", "Isolation_source"),
            fld("strain_description", "Strain_description"),
            fld("publication_reference", "Publication_reference"),
            fld("contact_researcher", "Contact_researcher"),
            fld("culture_collection_id", "Culture_collection_ID (alternative name[s])"),
            # note: these are free-text dates, there are comments mixed in
            fld(
                "culture_collection_date",
                "Culture_collection_date (YYYY-MM-DD)",
                coerce=date_or_comment,
            ),
            fld("host_location", "Host_location (state, country)"),
            fld("host_age", "Host_age", coerce=ingest_utils.int_or_comment),
            fld("host_dob", "Host_DOB (YYYY-MM-DD)", coerce=date_or_comment),
            fld("host_sex", "Host_sex (F/M)", coerce=get_sex),
            fld("host_disease_outcome", "Host_disease_outcome"),
            fld("host_description", "Host_description"),
        ]
        wrapper = ExcelWrapper(
            self._logger,
            field_spec,
            metadata_path,
            sheet_name=None,
            header_length=5,
            column_name_row_index=4,
        )
        for error in wrapper.get_errors():
            self._logger.error(error)
        return map_taxon_strain_rows(wrapper.get_all())


class SepsisGenomicsContextual:
    """
    Genomics sample metadata: used by the genomics classes.
    """

    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/2019-06-13/sample/"
    ]
    name = "sepsis-genomics"

    def __init__(self, logger, path):
        self._logger = logger
        xlsx_path = one(glob(path + "/*.xlsx"))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, sample_id, submission_obj):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        self._logger.warning(
            "no %s metadata available for: %s" % (type(self).__name__, repr(sample_id))
        )
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if not row.sample_id:
                continue
            if row.sample_id in sample_metadata:
                self._logger.warning(
                    "{}: duplicate sample metadata row for {}".format(
                        self.__class__.__name__, row.sample_id
                    )
                )
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                if field != "taxon_or_organism" and field != "strain_or_isolate":
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld("sample_id", "BPA_sample_ID", coerce=ingest_utils.extract_ands_id),
            fld("taxon_or_organism", "Taxon_OR_organism"),
            fld("strain_or_isolate", "Strain_OR_isolate"),
            fld("serovar", "Serovar", coerce=ingest_utils.int_or_comment),
            fld("growth_condition_time", "Growth_condition_time"),
            fld(
                "growth_condition_temperature",
                "Growth_condition_temperature",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("growth_condition_media", "Growth_condition_media"),
            fld("growth_condition_notes", "Growth_condition_notes"),
            fld(
                "experimental_replicate",
                "Experimental_replicate",
                coerce=ingest_utils.get_int,
            ),
            fld("analytical_facility", "Analytical_facility"),
            fld(
                "experimental_sample_preparation_method",
                "Experimental_sample_preparation_method",
            ),
            fld("data_type", "Data type"),
            fld("contact_researcher", "contact_researcher"),
        ]
        wrapper = ExcelWrapper(
            self._logger,
            field_spec,
            metadata_path,
            sheet_name="Genomics",
            header_length=4,
            column_name_row_index=3,
        )
        for error in wrapper.get_errors():
            self._logger.error(error)
        return wrapper.get_all()


class SepsisTranscriptomicsHiseqContextual:
    """
    Transcriptomics sample metadata: used by the genomics classes.
    """

    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/2019-06-13/sample/"
    ]
    name = "sepsis-transcriptomics-hiseq"

    def __init__(self, logger, path):
        self._logger = logger
        self.sample_metadata = {}
        for xlsx_path in glob(path + "/*.xlsx"):
            self.sample_metadata.update(
                self._package_metadata(self._read_metadata(xlsx_path))
            )

    def get(self, sample_id, submission_obj):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        self._logger.warning(
            "no %s metadata available for: %s" % (type(self).__name__, repr(sample_id))
        )
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if not row.sample_id:
                continue
            if row.sample_id in sample_metadata:
                self._logger.warning(
                    "{}: duplicate sample metadata row for {}".format(
                        self.__class__.__name__, row.sample_id
                    )
                )
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                if field != "taxon_or_organism" and field != "strain_or_isolate":
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld(
                "sample_submission_date",
                "sample submission date (yyyy-mm-dd)",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "sample_id",
                "sample name i.e. 5 digit bpa id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_type", "sample type"),
            fld("volume_ul", "volume (ul)"),
            fld("concentration_ng_per_ul", "concentration (ng/ul)"),
            fld("quantification_method", "quantification method"),
            fld("either_260_280", "260/280"),
            fld("taxon_or_organism", "taxon_or_organism"),
            fld("strain_or_isolate", "strain_or_isolate"),
            fld("serovar", "serovar", coerce=ingest_utils.int_or_comment),
            fld("growth_media", "growth media"),
            fld("replicate", "replicate", coerce=ingest_utils.get_int),
            fld("growth_condition_time", "growth_condition_time (h)"),
            fld("growth_condition_growth_phase", "growth_condition_growth phase"),
            fld("growth_condition_od600_reading", "growth_condition_od600 reading"),
            fld(
                "growth_condition_temperature",
                "growth_condition_temperature (°c)",
                units="\u00B0" + "C",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("growth_condition_media", "growth_condition_media"),
            fld("omics", "omics"),
            fld("analytical_platform", "analytical platform"),
            fld("facility", "facility"),
            fld("data_type", "data type"),
            fld("additional_notes", "additional notes"),
        ]

        wrapper = ExcelWrapper(
            self._logger,
            field_spec,
            metadata_path,
            sheet_name="RNA HiSeq",
            header_length=4,
            column_name_row_index=3,
        )
        for error in wrapper.get_errors():
            self._logger.error(error)
        return wrapper.get_all()


class SepsisMetabolomicsLCMSContextual:
    """
    Metabolomics sample metadata: used by the genomics classes.
    """

    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/2019-06-13/sample/"
    ]
    name = "sepsis-metabolomics-lcms"

    def __init__(self, logger, path):
        self._logger = logger
        self.sample_metadata = {}
        xlsx_path = one(glob(path + "/*.xlsx"))
        self.sample_metadata.update(
            self._package_metadata(self._read_metadata(xlsx_path))
        )

    def get(self, sample_id, submission_obj):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        self._logger.warning(
            "no %s metadata available for: %s" % (type(self).__name__, repr(sample_id))
        )
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if not row.sample_id:
                continue
            if row.sample_id in sample_metadata:
                self._logger.warning(
                    "{}: duplicate sample metadata row for {}".format(
                        self.__class__.__name__, row.sample_id
                    )
                )
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                if field != "taxon_or_organism" and field != "strain_or_isolate":
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld(
                "sample_submission_date",
                "sample submission date (yyyy-mm-dd)",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "sample_id",
                "sample name i.e. 5 digit bpa id",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("taxon_or_organism", "taxon_or_organism"),
            fld("strain_or_isolate", "strain_or_isolate"),
            fld("serovar", "serovar", coerce=ingest_utils.int_or_comment),
            fld("growth_media", "growth media"),
            fld("replicate", "replicate", coerce=ingest_utils.get_int),
            fld("growth_condition_time", "growth_condition_time (h)"),
            fld("growth_condition_growth_phase", "growth_condition_growth phase"),
            fld("growth_condition_od600_reading", "growth_condition_od600 reading"),
            fld(
                "growth_condition_temperature",
                "growth_condition_temperature (°c)",
                units="\u00B0" + "C",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("growth_condition_media", "growth_condition_media"),
            fld("omics", "omics"),
            fld("analytical_platform", "analytical platform"),
            fld("facility", "facility"),
            fld("data_type", "data type"),
            fld("additional_notes", "additional notes"),
        ]

        wrapper = ExcelWrapper(
            self._logger,
            field_spec,
            metadata_path,
            sheet_name="Metabolomics",
            header_length=4,
            column_name_row_index=3,
        )
        for error in wrapper.get_errors():
            self._logger.error(error)
        return wrapper.get_all()


class SepsisProteomicsBaseContextual:
    """
    Proteomics sample metadata: used by both proteomics classes.
    """

    metadata_urls = [
        "https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/2019-06-13/sample/"
    ]
    name = "sepsis-proteomics"

    def __init__(self, logger, path, analytical_platform):
        self._logger = logger
        self.analytical_platform = analytical_platform
        xlsx_path = one(glob(path + "/*.xlsx"))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, sample_id, submission_obj):
        if sample_id in self.sample_metadata:
            return self.sample_metadata[sample_id]
        self._logger.warning(
            "no %s metadata available for: %s" % (type(self).__name__, repr(sample_id))
        )
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if not row.sample_id:
                continue
            if row.analytical_platform.lower() != self.analytical_platform.lower():
                continue
            if row.sample_id in sample_metadata:
                self._logger.warning(
                    "{}: duplicate sample metadata row for {}".format(
                        self.__class__.__name__, row.sample_id
                    )
                )
            sample_metadata[row.sample_id] = row_meta = {}
            for field in row._fields:
                if field != "taxon_or_organism" and field != "strain_or_isolate":
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            fld(
                "sample_submission_date",
                "Sample submission date (YYYY-MM-DD)",
                coerce=ingest_utils.get_date_isoformat,
            ),
            fld(
                "sample_id",
                "Sample name i.e. 5 digit BPA ID",
                coerce=ingest_utils.extract_ands_id,
            ),
            fld("sample_type", "Sample type"),
            fld(
                "protein_yield_total",
                "protein yield - total (µg)",
                units="\u00B5" + "g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld(
                "protein_yield_facility",
                "protein yield / facility (µg)",
                units="\u00B5" + "g",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("treatment", "Treatment"),
            fld("peptide_resuspension_protocol", "Peptide resuspension protocol"),
            fld("taxon_or_organism", "Taxon_OR_organism"),
            fld("strain_or_isolate", "Strain_OR_isolate"),
            fld("serovar", "Serovar", coerce=ingest_utils.int_or_comment),
            fld("growth_media", "Growth Media"),
            fld("replicate", "Replicate", coerce=ingest_utils.get_int),
            fld("growth_condition_time", "Growth_condition_time"),
            fld("growth_condition_growth_phase", "Growth_condition_growth phase"),
            fld("growth_condition_od600_reading", "Growth_condition_OD600 reading"),
            fld(
                "growth_condition_temperature",
                "Growth_condition_temperature",
                coerce=ingest_utils.get_clean_number,
            ),
            fld("growth_condition_media", "Growth_condition_media"),
            fld("omics", "Omics"),
            fld("analytical_platform", "Analytical platform"),
            fld("facility", "Facility"),
            fld("data_type", "Data type"),
            fld("additional_notes", "additional notes"),
        ]
        wrapper = ExcelWrapper(
            self._logger,
            field_spec,
            metadata_path,
            sheet_name="Proteomics",
            header_length=4,
            column_name_row_index=3,
        )
        for error in wrapper.get_errors():
            self._logger.error(error)
        return wrapper.get_all()


class SepsisProteomicsMS1QuantificationContextual(SepsisProteomicsBaseContextual):
    def __init__(self, logger, path):
        self._logger = logger
        super().__init__(logger, path, "MS1 quantification")


class SepsisProteomicsSwathMSContextual(SepsisProteomicsBaseContextual):
    def __init__(self, logger, path):
        super().__init__(logger, path, "SWATH-MS")
