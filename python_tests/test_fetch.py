import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, DynamicDataClass


def test_memo_object_can_be_fetched_by_id(db0_fixture):
    object_1 = MemoTestClass(123)
    object_2 = db0.fetch(db0.uuid(object_1))
    assert object_2 is not None
    assert object_2.value == 123


def test_fetch_can_validate_memo_type(db0_fixture):
    object_1 = MemoTestClass(123)
    # fetching by incorrect type should raise
    with pytest.raises(Exception):
        db0.fetch(DynamicDataClass, db0.uuid(object_1))
    object_2 = db0.fetch(MemoTestClass, db0.uuid(object_1))
    assert object_2 is not None
    assert object_2.value == 123
