from collections import namedtuple
from .dbzero_ce import get_raw_prefixes


PrefixMetaData = namedtuple("PrefixMetaData", ["prefix_name", "prefix_uuid"])

def get_prefixes():
    for prefix in get_raw_prefixes():
        yield PrefixMetaData(*prefix)
