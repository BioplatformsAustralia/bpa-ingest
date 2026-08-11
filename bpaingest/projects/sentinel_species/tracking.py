from ...tracking import GoogleDriveTrackMetadata


class SentinelSpeciesGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Sentinel Species"
    skip_tracking_rows = 4
