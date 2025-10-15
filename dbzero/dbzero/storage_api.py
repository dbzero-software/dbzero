from collections import namedtuple
import dbzero as db0
from .dbzero import _get_prefixes, _get_current_prefix, _get_prefix_of, _get_mutable_prefixes


PrefixMetaData = namedtuple("PrefixMetaData", ["name", "uuid"])

def get_prefixes():
    for prefix in _get_prefixes():
        yield PrefixMetaData(*prefix)


def get_mutable_prefixes():
    for prefix in _get_mutable_prefixes():
        yield PrefixMetaData(*prefix)


def get_current_prefix():
    return PrefixMetaData(*_get_current_prefix())


def get_prefix_of(obj):
    return PrefixMetaData(*_get_prefix_of(obj))
