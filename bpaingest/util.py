import logging
import string


def bpa_id_to_ckan_name(s):
    return 'bpa-' + s.replace('/', '_').replace('.', '_')


def prune_dict(d, keys):
    if d is None:
        return None
    return dict((k, v) for (k, v) in d.items() if k in keys)


def clean_tag_name(s):
    "reduce s to strings acceptable in a tag name"
    return ''.join(t for t in s if t in string.digits or t in string.ascii_letters or t in '-_.')


def make_registration_decorator():
    """
    returns a (decorator, list). any function decorated with
    the returned decorator will be appended to the list
    """
    registered = []

    def _register(fn):
        registered.append(fn)
        return fn

    return _register, registered


def make_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    fmt = logging.Formatter("%(asctime)s [%(levelname)-5.5s] [%(threadName)s]  %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger
