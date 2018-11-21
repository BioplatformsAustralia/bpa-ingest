
import os
from glob import glob

from .util import make_logger, csv_to_named_tuple
from .libs import ingest_utils


logger = make_logger(__name__)


class NCBISRAContextual:
    """
    subclass this to get access to contextual metadata for NCBI SRA submissions
    currently used by the BASE and MM projects
    all subclasses must set a `bioproject_accession` class variable
    """
    metadata_patterns = [r'^.*\.(txt|csv)$']

    def __init__(self, path):
        self._path = path
        self.bpaid_biosample = {}
        self.file_submitted = set()
        # we have a few generations of data, import it all
        self.bpaid_biosample.update(
            self._read_2016_accessions())
        self.bpaid_biosample.update(
            self._read_accessions())
        self.file_submitted.update(
            self._read_2016_submitted())
        self.file_submitted.update(
            self._read_ncbi_sra())
        logger.info("NCBI upload metadata: %d files uploaded, %d biosample accessions" % (len(self.file_submitted), len(self.bpaid_biosample)))

    def _read_2016_accessions(self):
        fname = os.path.join(self._path, 'Biosample_accessions.csv')
        if not os.access(fname, os.R_OK):
            return {}
        _, biosample_rows = csv_to_named_tuple('BioSample', fname, mode='rU')
        return dict((ingest_utils.extract_ands_id(t.sample_name), t.accession.strip()) for t in biosample_rows)

    def _read_accessions(self):
        """
        BioSampleObjects.txt, produced by NCBI once the submission has been
        processed
        """
        sample_objects = glob(self._path + '/' + '*BioSampleObjects.txt')
        accessions = {}
        for fname in sample_objects:
            _, rows = csv_to_named_tuple('SRARow', fname, mode='rU', dialect='excel-tab')
            accessions.update(dict((ingest_utils.extract_ands_id(t.sample_name), t.accession) for t in rows))
        return accessions

    def _read_ncbi_sra(self):
        """
        SRA subtemplates as sent through to NCBI, TSV format
        """

        def yank_filenames(rows):
            for row in (t._asdict() for t in rows):
                for k, v in row.items():
                    if k.startswith('filename'):
                        yield v

        templates = glob(self._path + '/' + 'SRA_subtemplate*.txt')
        files = set()
        for fname in templates:
            _, rows = csv_to_named_tuple('SRARow', fname, mode='rU', dialect='excel-tab')
            files.update(yank_filenames(rows))
        return files

    def _read_2016_submitted(self):
        fname = os.path.join(self._path, 'files_submitted.csv')
        if not os.access(fname, os.R_OK):
            return {}
        _, upload_rows = csv_to_named_tuple('BioProject', fname, mode='rU')
        return {t.filename for t in upload_rows}

    def sample_ids(self):
        return list(self.bpaid_biosample.keys())

    def get(self, sample_id):
        # as a sample might be part of the Soil or non-Soil projects,
        # we must return None here if we don't have a definite match
        obj = {
            'ncbi_bioproject_accession': self.bioproject_accession,
        }
        if sample_id in self.bpaid_biosample:
            obj['ncbi_biosample_accession'] = self.bpaid_biosample[sample_id]
            return obj
        return None

    def filename_metadata(self, filename):
        return {
            'ncbi_file_uploaded': filename in self.file_submitted
        }
