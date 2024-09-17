import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, DynamicDataClass


def test_list_prefixes(db0_fixture):
    assert len(list(db0.get_prefixes())) == 1    
    db0.open("my-new_prefix")
    assert len(list(db0.get_prefixes())) == 2