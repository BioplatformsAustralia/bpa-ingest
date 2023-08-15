from ...tracking import GoogleDriveTrackMetadata


class AvianGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Avian Initiative"
    skip_tracking_rows = 4
