
import csv

from owslib.wfs import WebFeatureService
from bpaingest.util import make_logger
from bpaingest.projects.amdb.ingest import AccessAMDContextualMetadata
from bpaingest.metadata import DownloadMetadata
from _collections import defaultdict
logger = make_logger(__name__)

# IMOS_AM_MAP = {"IMOS_SITE_CODE": "IMOS_SITE_CODE",
#                "NRS_TRIP_CODE": "NRS_TRIP_CODE",
#                "NRS_SAMPLE_CODE": "NRS_SAMPLE_CODE",
#                "LATITUDE": "Latitude (decimal degrees)",
#                "LONGITUDE": "Longitude (decimal degrees)",
#                "SAMPLE_DEPTH_M": "Depth (m)"}

IMOS_AM_MAP = {"IMOS_SITE_CODE": "nrs_location_code_voyage_code",
               "NRS_TRIP_CODE": "nrs_trip_code",
               "NRS_SAMPLE_CODE": "nrs_sample_code",
               "LATITUDE": "latitude",
               "LONGITUDE": "longitude",
               "SAMPLE_DEPTH_M": "depth"}


class AODNData:
    """
    read the AODN-flavour CSV with metadata header
    """

    def __init__(self, fd):
        self.metadata_fields = self.fields = None
        self.metadata_rows = []
        self.rows = []
        self.extract_csv(csv.reader(fd))

    def extract_csv(self, data_sheet):
        for row in data_sheet:
            if not row:
                continue
            row = [t.strip() for t in row]

            # field metadata
            if row[0].startswith('#'):
                if len(row) < 1:
                    continue

                # fix for row with [#blankline]
                if not row[0][2:]:
                    continue

                row = [row[0][2:]] + row[1:]
                if self.metadata_fields is None:
                    self.metadata_fields = row
                else:
                    self.metadata_rows.append({k: v for (k, v) in zip(self.metadata_fields, row)})
            else:
                if self.fields is None:
                    self.fields = row
                else:
                    self.rows.append({k: v for (k, v) in zip(self.fields, row)})


class AMDContextualMetadata:

    def __init__(self):
        self.contextual_metadata = self.contextual_rows(AccessAMDContextualMetadata, name='amd-metadata')
        with open('am_contextual.csv', 'w') as amfile:
            writer = csv.DictWriter(amfile, self.contextual_metadata[0].keys())
            writer.writeheader()
            writer.writerows(self.contextual_metadata)

    def contextual_rows(self, ingest_cls, name):
        # flatten contextual metadata into dicts
        metadata = defaultdict(dict)
        with DownloadMetadata(ingest_cls, path='{}'.format(name)) as dlmeta:
            for contextual_source in dlmeta.meta.contextual_metadata:
                for sample_id in contextual_source.sample_ids():
                    metadata[sample_id]['sample_id'] = sample_id
                    metadata[sample_id].update(contextual_source.get(sample_id))

        def has_minimum_metadata(row):
            return 'latitude' in row and 'longitude' in row \
                and isinstance(row['latitude'], float) and isinstance(row['longitude'], float) \
                and 'nrs_trip_code' in row and 'nrs_sample_code' in row \
                and row['nrs_trip_code'] and row['nrs_sample_code']
        # convert into a row-like structure
        return list([t for t in metadata.values() if has_minimum_metadata(t)])


class AODNMetadata:
    wfs_server = 'https://geoserver-123.aodn.org.au/geoserver/ows'
    wfs_version = '1.1.0'
    layers = [
        'imos:anmn_nrs_bgc_chemistry_data',
        'imos:anmn_nrs_bgc_phypig_data',
        # comming below type as they don't have mapping columns
        # 'imos:anmn_nrs_bgc_tss_secchi_data',
        # 'imos:anmn_ctd_profiles_data'
    ]

    def __init__(self):
        self._wfs = WebFeatureService(url=self.wfs_server, version=self.wfs_version)
        # self._data = {layer: self._read_layer(layer) for layer in self.layers}
        self.imos_metadata = {}
        for layer in self.layers:
            self.imos_metadata[layer] = self._read_layer(layer)
            with open('{}.csv'.format(layer), 'w') as aodnfile:
                writer = csv.DictWriter(aodnfile, self.imos_metadata[layer].fields)
                writer.writeheader()
                writer.writerows(self.imos_metadata[layer].rows)

    def _read_layer(self, layer):
        logger.debug('reading {}'.format(layer))
        response = self._wfs.getfeature(
            typename=layer,
            outputFormat='csv-with-metadata-header',
            # bbox=(
            #     130, -13,
            #     131, -12),  # Darwin, FIXME hard-coded
            srsname='urn:x-ogc:def:crs:EPSG:4326'
        )
        return AODNData(response)


def main():
    aodn = AODNMetadata()
    aodn_data = []
    for layer in aodn.layers:
        for row in aodn.imos_metadata[layer].rows:
            aodn_data.append({k: row[k] for k in IMOS_AM_MAP.keys()})

    amd = AMDContextualMetadata()
    amd_data = []

    for row in amd.contextual_metadata:
        amd_dict = {k: row[k] for k in IMOS_AM_MAP.values()}
        amd_dict['sample_id'] = row['sample_id']
        amd_data.append(amd_dict)

    imos_am_map_data = [{**aitem, **bitem} for aitem in amd_data for bitem in aodn_data if aitem['nrs_sample_code'] ==
                        bitem['NRS_SAMPLE_CODE'] and aitem['nrs_trip_code'] == bitem['NRS_TRIP_CODE']]

    with open("imos-am-mapping.csv", "w") as mapfile:
        writer = csv.DictWriter(mapfile, imos_am_map_data[0].keys())
        writer.writeheader()
        writer.writerows(imos_am_map_data)


if __name__ == '__main__':
    main()
