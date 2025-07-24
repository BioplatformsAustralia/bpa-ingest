from ...tracking import GoogleDriveTrackMetadata


class FishGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Fish"
    skip_tracking_rows = 4
