import re
from ...libs import ingest_utils
from ...util import csv_to_named_tuple
from...util import make_logger

logger = make_logger(__name__)


class OMGSampleContextual(object):
    metadata_urls = ['https://downloads-qcif.bioplatforms.com/bpa/omg_staging/metadata/']
    metadata_patterns = [re.compile(r'^.*\.csv$')]
    name = 'omg-sample-contextual'

    def __init__(self, path):
        xlsx_path = path + '/OMG_GenomeSamples_metadata_genomes20170404.csv'
        self.sample_metadata = self._package_metadata(self._read_metadata(xlsx_path))

    def get(self, bpa_id):
        if bpa_id in self.sample_metadata:
            return self.sample_metadata[bpa_id]
        logger.warning("no %s metadata available for: %s" % (type(self).__name__, repr(bpa_id)))
        return {}

    def _package_metadata(self, rows):
        name_mapping = {
            'decimal_longitude': 'longitude',
            'decimal_latitude': 'latitude',
            'class_': 'class',
        }
        sample_metadata = {}
        for row in rows:
            assert(row.bpa_id)
            assert(row.bpa_id not in sample_metadata)
            bpa_id = ingest_utils.extract_bpa_id(row.bpa_id)
            sample_metadata[bpa_id] = row_meta = {}
            for field in row._fields:
                if field != 'bpa_id':
                    row_meta[name_mapping.get(field, field)] = getattr(row, field)
        return sample_metadata

    def _read_metadata(self, metadata_path):
        _, rows = csv_to_named_tuple('OMGSampleRow', metadata_path)
        return rows
