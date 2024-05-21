from ...tracking import GoogleDriveTrackMetadata, get_track_csv
from ...util import csv_to_named_tuple


class CollaborationsGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Collaborations"
    skip_tracking_rows = 0



class CollaborationsProjectsGoogleMetadata(GoogleDriveTrackMetadata):

    name = "Collaborations Project Codes"
    sheet_names = ["Collaborations Project Codes"]
    platform = "google-drive"
    parent_org = 'bpa-collaborations'
    skip_header_rows = 3

    def __init__(self, logger):
        self._logger = logger
        fname = get_track_csv(self.platform, "*" + self.name + "*.csv")
        logger.info("Reading track CSV file: " + fname)
        self.headers, self.project_code_rows = csv_to_named_tuple("CollaborationsProjectsGoogleMetadata",

                                                             fname, skip=self.skip_header_rows)

