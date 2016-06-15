import logging


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
    fmt = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger
