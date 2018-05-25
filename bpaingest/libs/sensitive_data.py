import re
import glob
import os.path
import shapely
import shapely.geometry
import fiona
from ..util import csv_to_named_tuple
from ..util import make_logger
from .ingest_utils import get_clean_number

logger = make_logger(__name__)

SENSITIVE_DATA_DIR = "sensitive_data"


class GeneralisationRules:
    WITHHOLD = re.compile("^WITHHOLD$")
    KM = re.compile("^(\d+)km$")


class ShapeFiles:
    AUS_DIR = "aus"
    AUS_STATES = "AUS_adm1.shp"
    # map the ids from shape file to the sensitive data location file
    AUS_LOCATION_MAP = {1: None,
                        2: "act",
                        3: None,
                        4: "nsw",
                        5: "nt",
                        6: "qld",
                        7: "sa",
                        8: "tas",
                        9: "vic",
                        11: "wa"}


class Generalisation:
    def __init__(self, generalisation_expression):
        self.expression = generalisation_expression
        self.withhold = False
        self.km = None
        if self.expression is not None:
            self._parse(self.expression)

    def apply(self, data):
        if self.withhold:
            data["location_generalisation"] = "WITHHOLD"
            if "latitude" in data:
                del data["latitude"]
            if "longitude" in data:
                del data["longitude"]
        elif self.km is not None:
            data["location_generalisation"] = "%skm" % self.km
            if "latitude" in data and "longitude" in data:
                lat = get_clean_number(data["latitude"])
                lng = get_clean_number(data["longitude"])

                data["latitude"], data["longitude"] = self._generalise(lat,
                                                                       lng,
                                                                       self.km)

    def _generalise(self, latitude, longitude, km):
        if km < 10:
            rounded_lat = round(latitude, 2)
            rounded_long = round(longitude, 2)
        elif km >= 10 and km < 100:
            rounded_lat = round(latitude, 1)
            rounded_long = round(longitude, 1)
        elif km > 100:
            rounded_lat = round(latitude, 0)
            rounded_long = round(longitude, 0)

        return rounded_lat, rounded_long

    def _parse(self, expression):
        if GeneralisationRules.WITHHOLD.match(expression):
            self.withhold = True
        else:
            m = GeneralisationRules.KM.match(expression)
            if m:
                self.km = int(m.groups()[0])
            else:
                logger.info("Unrecognised generalisation expression: %s" % expression)


class SensitiveDataGeneraliser:
    DEFAULT_GENERALISATION = "1km"

    def __init__(self):
        self.sensitive_files_path = self._get_sensitive_files_path()
        self.sensitive_species_map = {}
        self.shape_map = {}
        self._load_sensitive_species_data()
        self._load_shape_map()

    def apply(self, package):
        if "species" in package and "genus" in package:
            genus = package["genus"]
            species = package['species']
            scientific_name = "{0} {1}".format(genus, species).strip().lower()
            # optimisation: if this species isn't sensitive in any state, we can skip doing
            # expensive location determination
            if scientific_name not in self._all_scientific_names:
                return
            location = self._get_location(package)
            generalisation_expression = self._get_generalisation_expression(location,
                                                                            scientific_name)

            generalisation = Generalisation(generalisation_expression)
            generalisation.apply(package)

    def _get_sensitive_files_path(self):
        rel_path = "../../%s" % SENSITIVE_DATA_DIR
        return os.path.abspath(os.path.join(os.path.dirname(__file__),
                                            rel_path))

    def _get_generalisation_expression(self, location, scientific_name):
        data = self.sensitive_species_map.get(location, None)
        if data is None:
            return
        sn = scientific_name.lower()
        expression = data.get(sn, None)
        if expression is not None:
            logger.info("%s is sensitive! - expression = %s" % (sn, expression))

        return expression

    def _get_location(self, data):
        lat = get_clean_number(data["latitude"])
        lng = get_clean_number(data["longitude"])
        if lat is None or lng is None:
            return
        point = shapely.geometry.Point(lng, lat)
        aus_location = self._get_aus_location(point)
        if aus_location is not None:
            return aus_location

    def _load_sensitive_species_data(self):
        self.sensitive_species_map = {}
        for csv in glob.glob(os.path.join(self.sensitive_files_path, "*.csv")):
            location = os.path.splitext(os.path.basename(csv))[0]
            _, rows = csv_to_named_tuple('SensitiveData', csv)
            data = {}
            for row in rows:
                sn = row.scientificname.strip().lower()
                data[sn] = getattr(row, "generalisation", self.DEFAULT_GENERALISATION)
            self.sensitive_species_map[location] = data
        self._all_scientific_names = set()
        for species_map in self.sensitive_species_map.values():
            self._all_scientific_names |= set(species_map.keys())

    def _get_aus_location(self, point):
        for location, bounds, state_shape in self.shape_map[ShapeFiles.AUS_DIR]:
            if not bounds.contains(point):
                continue
            if state_shape.contains(point):
                return location

    def _load_shape_map(self):
        self.shape_map[ShapeFiles.AUS_DIR] = self._get_shapes(ShapeFiles.AUS_DIR,
                                                              ShapeFiles.AUS_STATES)

    def _get_location_name(self, subdir, shape_record):
        if subdir == 'aus':
            id = int(shape_record['id']) + 1
            location_name = ShapeFiles.AUS_LOCATION_MAP.get(id, None)
            return location_name

    def _get_shapes(self, subdir, shape_filename):
        shapes = []  # location, shape pairs
        shapefile_path = self._get_shapefile_path(subdir,
                                                  shape_filename)

        with fiona.open(shapefile_path) as coll:
            for state_shape_record in coll:
                shape = shapely.geometry.asShape(state_shape_record['geometry'])
                location = self._get_location_name(subdir, state_shape_record)
                if location is None:
                    continue
                minx, miny, maxx, maxy = shape.bounds
                bounds_poly = shapely.geometry.Polygon([
                    (minx, miny),
                    (minx, maxy),
                    (maxx, maxy),
                    (maxx, miny)])
                shapes.append((location, bounds_poly, shape))
        return shapes

    def _get_shapefile_path(self, subdir, shape_filename):
        rel_path = "../../%s/shapefiles/%s/%s" % (SENSITIVE_DATA_DIR,
                                                  subdir,
                                                  shape_filename)
        return os.path.abspath(
            os.path.join(os.path.dirname(__file__),
                         rel_path))
