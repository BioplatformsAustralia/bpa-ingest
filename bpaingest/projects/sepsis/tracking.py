from ...util import make_logger, csv_to_named_tuple, common_values
from ...libs import ingest_utils
from ...tracking import GoogleDriveTrackMetadata, get_track_csv
from collections import namedtuple, defaultdict

logger = make_logger(__name__)


class SepsisTrackMetadata(object):
    def __init__(self, name):
        fname = get_track_csv('bpam', '*' + name + '*.csv', project='sepsis')
        self.track_meta = self.read_track_csv(fname)

    def read_track_csv(self, fname):
        header, rows = csv_to_named_tuple('SepsisTrack', fname)
        return dict((ingest_utils.extract_ands_id(t.five_digit_bpa_id), t) for t in rows)

    def get(self, sample_id):
        if sample_id not in self.track_meta:
            logger.debug("No %s metadata for %s" % (type(self).__name__, sample_id))
            return {}
        track_meta = self.track_meta[sample_id]
        return {
            'data_type': track_meta.data_type,
            'taxon_or_organism': track_meta.taxon_or_organism,
            'strain_or_isolate': track_meta.strain_or_isolate,
            'serovar': track_meta.serovar,
            'growth_media': track_meta.growth_media,
            'replicate': track_meta.replicate,
            'omics': track_meta.omics,
            'work_order': track_meta.work_order,
            'contextual_data_submission_date': track_meta.contextual_data_submission_date,
            'sample_submission_date': track_meta.sample_submission_date,
            'archive_ingestion_date': track_meta.archive_ingestion_date,
            'archive_id': track_meta.archive_id,
        }


class SepsisGenomicsTrackMetadata(SepsisTrackMetadata):
    def get(self, sample_id):
        obj = super(SepsisGenomicsTrackMetadata, self).get(sample_id)
        track_meta = self.track_meta.get(sample_id)
        if track_meta:
            obj['growth_condition_notes'] = track_meta.growth_condition_notes
        return obj


class SepsisGoogleTrackMetadata(object):
    name = 'Antibiotic Resistant Pathogen'
    platform = 'google-drive'

    def __init__(self):
        fname = get_track_csv(self.platform, '*' + self.name + '*.csv')
        logger.info("Reading track CSV file: " + fname)
        self.headers, self.track_rows = self.read_track_csv(fname)
        self.track_meta = self.get_track_meta(self.track_rows)

    def read_track_csv(self, fname):
        headers, rows = csv_to_named_tuple('SepsisGoogleDriveTrack', fname)
        # Sepsis has multiple row for one ticket in googledrive spreadsheet. See github issue -https://github.com/BioplatformsAustralia/bpa-archive-ops/issues/698
        track_rows = defaultdict(list)
        # Grouping rows per ticket
        for row in rows:
            track_rows[row.ccg_jira_ticket].append(row)
        return headers, track_rows

    def get_track_meta(self, track_rows):
        track_meta = {}
        # Getting all fields with unique values for the given ticket
        for ticket_id, meta_list in track_rows.items():
            track_meta[ticket_id] = common_values([meta._asdict() for meta in meta_list])
        # These fields have differing values for a given ticket, but the sorted set of unique values does have meaning to the user
        for ticket_id, meta_list in track_rows.items():
            for field in ('description', 'date_of_transfer_to_archive', 'growth_media'):
                vals = set(getattr(meta, field) for meta in meta_list)
                track_meta[ticket_id][field] = ', '.join(sorted(vals))
        return track_meta

    def get(self, ticket):
        # wrapping common values(dict) into a single object
        custom_obj_type = namedtuple("SepsisGoogleDriveTrackCommonMeta", self.track_meta[ticket].keys())
        return custom_obj_type(*(self.track_meta[ticket].values()))

    def get_taxons_strains(self, ticket):
        taxons = defaultdict(list)
        strains = defaultdict(list)
        meta_list = self.track_rows[ticket]
        taxons = list(getattr(meta, 'taxon_or_organism') for meta in meta_list)
        strains = list(getattr(meta, 'strain_or_isolate') for meta in meta_list)
        return taxons, strains
