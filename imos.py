
from owslib.wfs import WebFeatureService


def main():
    imos = WebFeatureService(url='https://geoserver-123.aodn.org.au/geoserver/ows', version='1.1.0')

    layers = (
        'imos:anmn_nrs_bgc_chemistry_data',
        'imos:anmn_nrs_bgc_phypig_data',
        'imos:anmn_nrs_bgc_tss_secchi_data',
        'imos:anmn_ctd_profiles_data')

    for layer in layers:
        response = imos.getfeature(
            typename=layer,
            outputFormat='csv-with-metadata-header',
            bbox=(
                -43.6345972634, 113.338953078,
                -10.6681857235, 153.569469029),
            srsname='urn:x-ogc:def:crs:EPSG:4326')
        print(response.read())


if __name__ == '__main__':
    main()
