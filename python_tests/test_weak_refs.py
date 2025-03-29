import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestPxClass


def test_referencing_foreign_object_raises_error(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    with pytest.raises(Exception):
        # exception due to attept to reference obj_1 from obj_2 (which is on another prefix)
        obj_2 = MemoTestPxClass(obj_1, prefix=px_2)


def test_referencing_foreign_object_with_weak_proxy(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)    
    assert obj_2 is not None


def test_unreferencing_object_by_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)    
    assert obj_2.value.value == 123
