from ...tracking import GoogleDriveTrackMetadata


class FishGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Fish Initiative"
    skip_tracking_rows = 4
