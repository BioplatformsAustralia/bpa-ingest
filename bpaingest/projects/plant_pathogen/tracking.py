from ...tracking import GoogleDriveTrackMetadata
from ...tracking import GoogleDriveTrackMetadata, get_track_csv
from ...util import csv_to_named_tuple


class PlantPathogenGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Plant Pathogen"
    skip_tracking_rows = 4


class PlantPathogenProjectsGoogleMetadata(GoogleDriveTrackMetadata):

    name = "Plant Pathogen Project Codes"
    sheet_names = ["Plant Pathogen Project Codes"]
    platform = "google-drive"
    parent_org = 'pp-consortium-members'
    skip_header_rows = 3

    def __init__(self, logger):
        self._logger = logger
        fname = get_track_csv(self.platform, "*" + self.name + "*.csv")
        logger.info("Reading track CSV file: " + fname)
        self.headers, self.project_code_rows = csv_to_named_tuple("PlantPathogenProjectsGoogleMetadata", fname, skip=self.skip_header_rows)



