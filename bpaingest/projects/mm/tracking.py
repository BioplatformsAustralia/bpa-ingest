from ...util import make_logger, csv_to_named_tuple
from ...libs import ingest_utils
from ...tracking import GoogleDriveTrackMetadata, get_track_csv


logger = make_logger(__name__)


class MarineMicrobesTrackMetadata(object):
    def __init__(self, name):
        fname = get_track_csv('bpam', '*' + name + '*.csv', project='marine-microbes')
        self.track_meta = self.read_track_csv(fname)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('MarineMicrobesTrack', fname)
        return dict((ingest_utils.extract_bpa_id(t.five_digit_bpa_id), t) for t in rows)

    def get(self, bpa_id):
        data = {}

        if bpa_id not in self.track_meta:
            logger.debug("No %s metadata for %s" % (type(self).__name__, bpa_id))
            data = {
                'sample_type': '',
                'costal_id': '',
                'contextual_data_submission_date': '',
                'sample_submission_date': '',
                'submitter': '',
                'work_order': '',
                'data_generated': '',
                'facility': '',
                'archive_ingestion_date': '',
                'file_name': '',
            }
            return data

        track_meta = self.track_meta[bpa_id]
        data = {
            'sample_type': track_meta.sample_type,
            'costal_id': track_meta.costal_id.strip(),
            'contextual_data_submission_date': track_meta.contextual_data_submission_date,
            'sample_submission_date': track_meta.sample_submission_date,
            'submitter': track_meta.submitter,
            'work_order': track_meta.work_order,
            'data_generated': track_meta.data_generated.strip(),
            'facility': track_meta.facility.strip(),
            'archive_ingestion_date': track_meta.archive_ingestion_date,
            'file_name': track_meta.file_name.strip(),
        }

        return data


class MarineMicrobesGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = 'Marine Microbe'
