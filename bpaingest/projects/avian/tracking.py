from ...tracking import GoogleDriveTrackMetadata


class AvianGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Avian"
    skip_tracking_rows = 4
