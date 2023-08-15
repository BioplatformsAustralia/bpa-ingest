from ...tracking import GoogleDriveTrackMetadata


class PlantProteinAtlasGoogleTrackMetadata(GoogleDriveTrackMetadata):
    name = "Plant Protein Atlas"
    skip_tracking_rows = 4
