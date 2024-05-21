import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


def test_fields_can_be_retrieved_from_memo_type(db0_fixture):
    fields = MemoTestClass.__fields__
    assert fields is not None    


def test_fields_can_be_used_to_access_memo_type_defs(db0_fixture):
    # an instance must be created to initialize type fields
    object = MemoTestClass(123)
    field_def = MemoTestClass.__fields__.value
    assert field_def is not None
