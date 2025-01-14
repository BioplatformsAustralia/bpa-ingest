from ...tracking import GoogleDriveTrackMetadata


class AusArgGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "AusARG"
    skip_tracking_rows = 4
