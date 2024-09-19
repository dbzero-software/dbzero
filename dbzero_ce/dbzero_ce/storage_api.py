from collections import namedtuple
import dbzero_ce as db0
from .dbzero_ce import get_raw_prefixes, get_raw_current_prefix


PrefixMetaData = namedtuple("PrefixMetaData", ["name", "uuid"])

def get_prefixes():
    for prefix in get_raw_prefixes():
        yield PrefixMetaData(*prefix)


def get_current_prefix():
    return PrefixMetaData(*get_raw_current_prefix())
