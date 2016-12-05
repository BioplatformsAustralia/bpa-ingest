from ...util import make_logger, one
from ...libs import ingest_utils
from ...libs.excel_wrapper import ExcelWrapper
from glob import glob


logger = make_logger(__name__)


def get_gram_stain(val):
    if val and val is not '':
        val = val.lower()
        if 'positive' in val:
            return 'POS'
        elif 'negative' in val:
            return 'NEG'
    return None


def get_sex(val):
    if val and val is not '':
        val = val.lower()
        # order of these statements is significant
        if 'female' in val:
            return 'F'
        if 'male' in val:
            return 'M'
    return None


def get_strain_or_isolate(val):
    if val and val is not '':
        # convert floats to str
        if isinstance(val, float):
            val = int(val)
        return str(val)
    return None


class SepsisBacterialContextual(object):
    """
    Bacterial sample metadata: used by each of the -omics classes below.
    """

    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/current/bacterial/']
    name = 'sepsis-bacterial'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id, submission_obj):
        tpl = (submission_obj['taxon_or_organism'], submission_obj['strain_or_isolate'])
        if tpl in self.sample_metadata:
            return self.sample_metadata[tpl]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(tpl)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if row.taxon_or_organism is None or row.strain_or_isolate is None:
                continue
            strain_tuple = (row.taxon_or_organism, row.strain_or_isolate)
            assert(strain_tuple not in sample_metadata)
            sample_metadata[strain_tuple] = row_meta = {}
            for field in row._fields:
                if field != 'taxon_or_organism' and field != 'strain_or_isolate':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('gram_stain', 'Gram_staining_(positive_or_negative)', get_gram_stain),
            ('taxon_or_organism', 'Taxon_OR_organism', None),
            ('strain_or_isolate', 'Strain_OR_isolate', get_strain_or_isolate),
            ('serovar', 'Serovar', None),
            ('key_virulence_genes', 'Key_virulence_genes', None),
            ('isolation_source', 'Isolation_source', None),
            ('strain_description', 'Strain_description', None),
            ('publication_reference', 'Publication_reference', None),
            ('contact_researcher', 'Contact_researcher', None),
            ('culture_collection_id', 'Culture_collection_ID (alternative name[s])', None),
            ('culture_collection_date', 'Culture_collection_date (YYYY-MM-DD)', ingest_utils.get_date_isoformat),
            ('host_location', 'Host_location (state, country)', None),
            ('host_age', 'Host_age', ingest_utils.get_int),
            ('host_dob', 'Host_DOB (YYYY-MM-DD)', ingest_utils.get_date_isoformat),
            ('host_sex', 'Host_sex (F/M)', get_sex),
            ('host_disease_outcome', 'Host_disease_outcome', None),
            ('host_description', 'Host_description', None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=None,
            header_length=5,
            column_name_row_index=4,
            formatting_info=True)
        return wrapper.get_all()


class SepsisGenomicsContextual(object):
    """
    Genomics sample metadata: used by the genomics classes.
    """

    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/current/genomics/']
    name = 'sepsis-genomics'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id, submission_obj):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(bpa_id)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if not row.bpa_id:
                continue
            if row.bpa_id not in sample_metadata:
                logger.warning("duplicate sample metadata row for {}".format(row.bpa_id))
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'taxon_or_organism' and field != 'strain_or_isolate':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('bpa_id', "BPA_sample_ID", ingest_utils.extract_bpa_id),
            ('taxon_or_organism', "Taxon_OR_organism", None),
            ('strain_or_isolate', "Strain_OR_isolate", None),
            ('serovar', "Serovar", None),
            ('growth_condition_time', "Growth_condition_time", None),
            ('growth_condition_temperature', "Growth_condition_temperature", ingest_utils.get_clean_number),
            ('growth_condition_media', "Growth_condition_media", None),
            ('growth_condition_notes', "Growth_condition_notes", None),
            ('experimental_replicate', "Experimental_replicate", None),
            ('analytical_facility', "Analytical_facility", None),
            ('analytical_platform', "Analytical_platform", None),
            ('experimental_sample_preparation_method', "Experimental_sample_preparation_method", None),
            ('data_type', "Data type", None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name=None,
            header_length=5,
            column_name_row_index=4,
            formatting_info=True)
        return wrapper.get_all()


class SepsisTranscriptomicsHiseqContextual(object):
    """
    Genomics sample metadata: used by the genomics classes.
    """

    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/current/other-omics/']
    name = 'sepsis-transcriptomics-hiseq'

    def __init__(self, path):
        self.sample_metadata = {}
        for xlsx_path in glob(path + '/*.xlsx'):
            self.sample_metadata.update(self._package_metadata(self._read_metadata(xlsx_path)))

    def get(self, bpa_id, submission_obj):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(bpa_id)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if not row.bpa_id:
                continue
            if row.bpa_id not in sample_metadata:
                logger.warning("duplicate sample metadata row for {}".format(row.bpa_id))
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'taxon_or_organism' and field != 'strain_or_isolate':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('sample_submission_date', 'Sample submission date (YYYY-MM-DD)', None),
            ('bpa_id', 'Sample name i.e. 5 digit BPA ID', ingest_utils.extract_bpa_id),
            ('sample_type', 'Sample type', None),
            ('volume_ul', 'Volume (ul)', None),
            ('concentration_ng_per_ul', 'Contentration (ng/ul)', None),
            ('quantification_method', 'Quantification method', None),
            ('either_260_280', '260/280', None),
            ('taxon_or_organism', 'Taxon_OR_organism', None),
            ('strain_or_isolate', 'Strain_OR_isolate', None),
            ('serovar', 'Serovar', None),
            ('growth_media', 'Growth Media', None),
            ('replicate', 'Replicate', None),
            ('growth_condition_time', 'Growth_condition_time', None),
            ('growth_condition_temperature', "Growth_condition_temperature", ingest_utils.get_clean_number),
            ('growth_condition_media', 'Growth_condition_media', None),
            ('omics', 'Omics', None),
            ('analytical_platform', 'Analytical platform', None),
            ('facility', 'Facility', None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name='RNA HiSeq',
            header_length=4,
            column_name_row_index=3,
            formatting_info=True)
        return wrapper.get_all()


class SepsisMetabolomicsLCMSContextual(object):
    """
    Genomics sample metadata: used by the genomics classes.
    """

    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/current/other-omics/']
    name = 'sepsis-metabolomics-lcms'

    def __init__(self, path):
        self.sample_metadata = {}
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata.update(self._package_metadata(self._read_metadata(xlsx_path)))

    def get(self, bpa_id, submission_obj):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(bpa_id)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if not row.bpa_id:
                continue
            if row.bpa_id not in sample_metadata:
                logger.warning("duplicate sample metadata row for {}".format(row.bpa_id))
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'taxon_or_organism' and field != 'strain_or_isolate':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):

        field_spec = [
            ('sample_submission_date', 'Sample submission date (YYYY-MM-DD)', None),
            ('bpa_id', 'Sample name i.e. 5 digit BPA ID', ingest_utils.extract_bpa_id),
            ('taxon_or_organism', 'Taxon_OR_organism', None),
            ('strain_or_isolate', 'Strain_OR_isolate', None),
            ('serovar', 'Serovar', None),
            ('growth_media', 'Growth Media', None),
            ('replicate', 'Replicate', None),
            ('growth_condition_time', 'Growth_condition_time', None),
            ('growth_condition_temperature', "Growth_condition_temperature", ingest_utils.get_clean_number),
            ('growth_condition_media', 'Growth_condition_media', None),
            ('omics', 'Omics', None),
            ('analytical_platform', 'Analytical platform', None),
            ('facility', 'Facility', None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name='Metabolomics',
            header_length=4,
            column_name_row_index=3,
            formatting_info=True)
        return wrapper.get_all()


class SepsisProteomicsContextual(object):
    """
    Proteomics sample metadata: used by both proteomics classes.
    """

    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/sepsis/projectdata/current/other-omics/']
    name = 'sepsis-proteomics'

    def __init__(self, path):
        xlsx_path = one(glob(path + '/*.xlsx'))
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id, submission_obj):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(bpa_id)))
        return {}

    def _package_metadata(self, rows):
        sample_metadata = {}
        for row in rows:
            if not row.bpa_id:
                continue
            if row.bpa_id not in sample_metadata:
                logger.warning("duplicate sample metadata row for {}".format(row.bpa_id))
            sample_metadata[row.bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'taxon_or_organism' and field != 'strain_or_isolate':
                    row_meta[field] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        field_spec = [
            ('sample_submission_date', 'Sample submission date (YYYY-MM-DD)', None),
            ('bpa_id', 'Sample name i.e. 5 digit BPA ID', ingest_utils.extract_bpa_id),
            ('sample_type', 'Sample type', None),
            ('protein_yield_ug', 'Protein Yield (g)', None),  # it really is ug, just unicode stripping drops the 'u'
            ('treatment', 'Treatment', None),
            ('peptide_resuspension_protocol', 'Peptide resuspension protocol', None),
            ('taxon_or_organism', 'Taxon_OR_organism', None),
            ('strain_or_isolate', 'Strain_OR_isolate', None),
            ('serovar', 'Serovar', None),
            ('growth_media', 'Growth Media', None),
            ('replicate', 'Replicate', None),
            ('growth_condition_time', 'Growth_condition_time', None),
            ('growth_condition_growth_phase', 'Growth_condition_growth phase', None),
            ('growth_condition_od600_reading', 'Growth_condition_OD600 reading', None),
            ('growth_condition_temperature', 'Growth_condition_temperature', ingest_utils.get_clean_number),
            ('growth_condition_media', 'Growth_condition_media', None),
            ('omics', 'Omics', None),
            ('analytical_platform', 'Analytical platform', None),
            ('facility', 'Facility', None),
            ('data_type', 'Data type', None),
        ]
        wrapper = ExcelWrapper(
            field_spec,
            metadata_path,
            sheet_name='Proteomics',
            header_length=4,
            column_name_row_index=3,
            formatting_info=True)
        return wrapper.get_all()
