from ...tracking import GoogleDriveTrackMetadata
from ...tracking import GoogleDriveTrackMetadata, get_track_csv
from ...util import csv_to_named_tuple


class AnimalDiseaseGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Animal Disease"
    skip_tracking_rows = 4


class AnimalDiseaseProjectsGoogleMetadata(GoogleDriveTrackMetadata):
    name = "AD Project Codes"
    sheet_names = ["Animal Disease Project Codes"]
    platform = "google-drive"
    parent_org = "ad-consortium-members"
    skip_header_rows = 3

    def __init__(self, logger):
        self._logger = logger
        fname = get_track_csv(self.platform, "*" + self.name + "*.csv")
        logger.info("Reading track CSV file: " + fname)
        self.headers, self.project_code_rows = csv_to_named_tuple(
            "AnimalDiseaseProjectsGoogleMetadata", fname, skip=self.skip_header_rows
        )
