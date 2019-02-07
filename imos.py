
import csv

from owslib.wfs import WebFeatureService
from bpaingest.util import make_logger

logger = make_logger(__name__)


class AODNData:
    """
    read the AODN-flavour CSV with metadata header
    """

    def __init__(self, fd):
        reader = csv.reader(fd)
        metadata_fields = fields = None
        metadata_rows = []
        rows = []
        for row in reader:
            if not row:
                continue
            row = [t.strip() for t in row]

            # field metadata
            if row[0].startswith('#'):
                if len(row) < 1:
                    continue
                row = [row[0][2:]] + row[1:]
                if metadata_fields is None:
                    metadata_fields = row
                else:
                    metadata_rows.append({k: v for (k, v) in zip(metadata_fields, row)})
            else:
                if fields is None:
                    fields = row
                else:
                    rows.append({k: v for (k, v) in zip(fields, row)})

        from pprint import pprint
        pprint(metadata_rows)
        self.fields = {
            row['data_column_name']: row
            for row in metadata_rows
        }

        logger.debug(self.fields)


class AODNMetadata:
    wfs_server = 'https://geoserver-123.aodn.org.au/geoserver/ows'
    wfs_version = '1.1.0'
    layers = [
        'imos:anmn_nrs_bgc_chemistry_data',
        # 'imos:anmn_nrs_bgc_phypig_data',
        # 'imos:anmn_nrs_bgc_tss_secchi_data',
        # 'imos:anmn_ctd_profiles_data'
    ]

    def __init__(self):
        self._wfs = WebFeatureService(url=self.wfs_server, version=self.wfs_version)
        self._data = {layer: self._read_layer(layer) for layer in self.layers}

    def _read_layer(self, layer):
        logger.debug('reading {}'.format(layer))
        response = self._wfs.getfeature(
            typename=layer,
            outputFormat='csv-with-metadata-header',
            bbox=(
                130, -13,
                131, -12),  # Darwin, FIXME hard-coded
            srsname='urn:x-ogc:def:crs:EPSG:4326')
        return AODNData(response)


def main():
    _ = AODNMetadata()


if __name__ == '__main__':
    main()
