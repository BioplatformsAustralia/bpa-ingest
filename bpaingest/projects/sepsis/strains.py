#
# late in the project, several (taxon_or_organism, strain_or_isolate) tuples were
# renamed. as there are a large number of metadata sources with the old names, and
# it is impractical to edit them all, we instead deal with this issue in code.
#
# the approach is to map the tuples in all metadata sources, at read time. remapping
# at output time is not a viable approach, as some of the metadata sources (e.g. the
# strain metadata, plus later analysed data) use the new names. by mapping everything
# on input, we have can rely on the new strain names when linking different metadata
# sources together
#
# this is a strict one to one mapping.
#
# Notes:
# (1) if we are reading in a bunch of data and using common_values, we may end up with
# only a strain or taxon. in such case we can't be sure it is correct (might be one
# of the mapped ones) so we suppress both values
#

from ...util import make_logger
from collections import OrderedDict


logger = make_logger(__name__)

TAXON_STRAIN_MAPPING = {
    ("Klebsiella pneumoniae", "AJ055"): ("Klebsiella variicola", "AJ055"),
    ("Klebsiella pneumoniae", "AJ292"): ("Klebsiella variicola", "AJ292"),
    ("Klebsiella pneumoniae", "03-311-0071"): ("Klebsiella variicola", "03-311-0071"),
    ("Klebsiella pneumoniae", "04153260899A"): ("Klebsiella variicola", "04153260899A"),
}


def empty_to_null(s):
    if s == "":
        return None
    return s


def get_taxon_strain(obj):
    tpl = tuple(
        map(empty_to_null, (obj.get("taxon_or_organism"), obj.get("strain_or_isolate")))
    )
    # see (1) above
    if None in tpl:
        return None, None
    return tpl


def map_taxon_strain(taxon, strain):
    tpl = (taxon, strain)
    # see (1) above
    if None in tpl:
        return None, None
    # return the mapping, or otherwise the original value
    return TAXON_STRAIN_MAPPING.get(tpl, tpl)


def map_taxon_strain_dict(obj):
    """
    modify a dict (in-place), updating the 'strain_or_isolate' and 'taxon_or_organism'
    fields (if present)
    """
    assert type(obj) is dict or type(obj) is OrderedDict
    taxon, strain = map_taxon_strain(*get_taxon_strain(obj))
    obj["taxon_or_organism"] = taxon
    obj["strain_or_isolate"] = strain


def map_taxon_strain_rows(row_iter):
    """
    modify namedtuple instances coming from an iterable way as a list
    """
    mapped = []
    for row in row_iter:
        if not hasattr(row, "strain_or_isolate") or not hasattr(
            row, "taxon_or_organism"
        ):
            mapped.append(row)
            continue
        obj = row._asdict()
        map_taxon_strain_dict(obj)
        mapped.append(type(row)(**obj))
    return mapped
