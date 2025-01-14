import math

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
        warningWords = ""
        for keyname in ["subspecies_or_variant", "subspecies"]:
            if keyname in package:
                return package.get(keyname)

            warnValue = package.get("sample_id")
            if warnValue is None:
                warnValue = package.get("bpa_sample_id")
            if warnValue is None:
                warnValue = package.get("dataset_id")
            if warnValue is None:
                warnValue = package.get("bpa_dataset_id")
            if warnValue is None:
                warnValue = package.get("libary_id")
            if warnValue is None:
                warnValue = package.get("bpa_library_id")
            if warnValue is None:
                warnValue = package.get("ticket")
            if warnValue is None:
                warnValue = package

            warningWords = f"Unable to find subspecies in {warnValue}"

        self._logger.warn(warningWords)

    def species_name(self, package):
        if package.get("genus", "") and package.get("species", ""):
            return "{} {}".format(package.get("genus", ""), package.get("species", ""))
        warnValue = package.get("sample_id")
        if warnValue is None:
            warnValue = package.get("bpa_sample_id")
        if warnValue is None:
            warnValue = package.get("dataset_id")
        if warnValue is None:
            warnValue = package.get("bpa_dataset_id")
        if warnValue is None:
            warnValue = package.get("libary_id")
        if warnValue is None:
            warnValue = package.get("bpa_library_id")
        if warnValue is None:
            warnValue = package.get("ticket")
        if warnValue is None:
            warnValue = package

        warningWords = f"Unable to find species in {warnValue}"

        self._logger.warn(warningWords)

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
                self._logger.debug(
                    f"ID: {package.get(self.package_id_keyname, '')} has 'key:value' pair 'country: {country}', which is outside Australia. Suppressing."
                )
                package.update({"latitude": None, "longitude": None})
                continue
            generalised = self.get_generalised(package, cache)
            if generalised:
                package.update(generalised._asdict())

        return packages

    def get_generalised(self, package, cache):
        # Sample is in Australia; use ALA to determine whether it is sensitive,
        # and apply the relevant sensitisation level (if any). Otherwise return original.

        lat, lng = (
            get_clean_number(self._logger, package.get("latitude")),
            get_clean_number(self._logger, package.get("longitude")),
        )
        generalised = self.update_cache(cache, (self.species_name(package), lat, lng))
        if not generalised:
            generalised = self.update_cache(
                cache, (self.subspecies_name(package), lat, lng)
            )
        self.validate_generalised(package, generalised)
        return generalised

    def validate_generalised(self, package, generalised):
        if not generalised:
            return
        if not self.validate_rounding(
            package.get("latitude"), generalised.latitude
        ) or not self.validate_rounding(
            package.get("longitude"), generalised.longitude
        ):
            raise Exception(
                f"The base numbers for generalised lat and long do not match the originals. {self.log_verbose_identifiers(package)} NOT represented by generalised lat:{generalised.latitude}, long:{generalised.longitude}"
            )
        ## if validation passes, log any legitimate 'rounding' updates to lat and long
        if float(package.get("latitude")) != float(generalised.latitude) and float(
            package.get("longitude")
        ) != float(generalised.longitude):
            self._logger.info(
                f"{self.log_verbose_identifiers(package)} generalised to: {generalised._asdict()}"
            )

    def validate_rounding(self, original, rounded):
        return math.floor(float(original)) <= rounded <= math.ceil(float(original))

    def update_cache(self, cache, args):
        if args not in cache:
            cache[args] = self.generaliser.apply(*args)
        return cache[args]

    def log_verbose_identifiers(self, package):
        return f"id: {package.get(self.package_id_keyname, '')}, lat: {package.get('latitude')}, long: {package.get('longitude')}"
