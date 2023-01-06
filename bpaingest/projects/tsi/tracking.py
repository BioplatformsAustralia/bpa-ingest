from ...tracking import GoogleDriveTrackMetadata


class TSIGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "TSI"
    skip_tracking_rows = 4
