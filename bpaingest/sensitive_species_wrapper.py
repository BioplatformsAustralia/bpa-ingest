from bpaingest.libs.ingest_utils import get_clean_number
from bpasslh.handler import SensitiveDataGeneraliser


class SensitiveSpeciesWrapper:
    def __init__(self, logger, *args, **kwargs):
        self.generaliser = SensitiveDataGeneraliser(logger)
        self.package_id_keyname = kwargs.get("package_id_keyname", "bpa_dataset_id")
        self._logger = logger

    def get_species_and_sub_species(self, packages):
        collected = []
        for p in packages:
            species = self.species_name(p)
            if species and species not in collected:
                collected.append(species)
                subspecies = self.subspecies_name(p)
                if subspecies and subspecies not in collected:
                    collected.append(subspecies)
        return collected

    def subspecies_name(self, package):
        for keyname in ["subspecies_or_variant", "subspecies"]:
            if keyname in package:
                return package.get(keyname)
        self._logger.warn(
            f"Unable to find subspecies in {package.get('sample_id') or package.get('bpa_sample_id')}"
        )

    def species_name(self, package):
        return "{} {}".format(package.get("genus", ""), package.get("species", ""))

    def apply_location_generalisation(self, packages):
        "Apply location generalisation for sensitive species found from ALA"

        # prime the cache of responses
        self._logger.info("building location generalisation cache")
        names = sorted(set(self.get_species_and_sub_species(packages)))
        self.generaliser.ala_lookup.get_bulk(names)

        cache = {}
        for package in packages:
            # if the sample wasn't collected in Australia, suppress the longitude
            # and latitude (ALA lookup via SSLH is irrelevant)
            country = package.get("country", "")
            if country.lower() != "australia":
                self._logger.info(
                    "library_id {} outside Australia, suppressing location: {}".format(
                        package.get(self.package_id_keyname, ""), country
                    )
                )
                package.update({"latitude": None, "longitude": None})
                continue

            generalised = self.get_generalised(package, cache)
            if generalised:
                self._logger.debug(f"generalised package ID is: {package.get('id')}")
                self._logger.debug(f"generalised to: {generalised._asdict()}")
                package.update(generalised._asdict())

        return packages

    def get_generalised(self, package, cache):
        # Sample is in Australia; use ALA to determine whether it is sensitive,
        # and apply the relevant sensitisation level (if any)

        lat, lng = (
            get_clean_number(self._logger, package.get("latitude")),
            get_clean_number(self._logger, package.get("longitude")),
        )
        generalised = self.update_cache(cache, (self.species_name(package), lat, lng))
        if not generalised:
            generalised = self.update_cache(
                cache, (self.subspecies_name(package), lat, lng)
            )
        return generalised

    def update_cache(self, cache, args):
        if args not in cache:
            cache[args] = self.generaliser.apply(*args)
        return cache[args]
