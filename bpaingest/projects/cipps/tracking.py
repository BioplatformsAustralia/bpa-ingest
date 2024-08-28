from ...tracking import GoogleDriveTrackMetadata, get_track_csv
from ...util import csv_to_named_tuple


class CIPPSGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "CIPPS"
    skip_tracking_rows = 4


class CIPPSProjectsGoogleMetadata(GoogleDriveTrackMetadata):
    name = "CIPPS Project Codes"
    sheet_names = ["CIPPS Project Codes"]
    platform = "google-drive"
    parent_org = "cipps-consortium-members"
    skip_header_rows = 2

    def __init__(self, logger):
        self._logger = logger
        fname = get_track_csv(self.platform, "*" + self.name + "*.csv")
        logger.info("Reading track CSV file: " + fname)
        self.headers, self.project_code_rows = csv_to_named_tuple(
            "CIPPSProjectsGoogleMetadata", fname, skip=self.skip_header_rows
        )
