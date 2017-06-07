# in the case of MM, this is the metadata coming over from the
# BPA Projects Data Transfer Summary on Google Drive, at least for now
# key is the CCG Jira Ticket column

from ...util import make_logger, csv_to_named_tuple


logger = make_logger(__name__)


class MarineMicrobesTrackMetadata(object):
    def __init__(self, track_csv_path):
        self.track_meta = self.read_track_csv(track_csv_path)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('MarineMicrobesTrack', fname)
        return dict((t.ccg_jira_ticket.strip().lower(), t) for t in rows)

    def get(self, ticket):
        return self.track_meta[ticket.strip().lower()]
