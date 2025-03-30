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
    assert db0.getrefcount(obj_1) == 0
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)    
    assert obj_2 is not None
    # make sure ref-count is not incremented
    assert db0.getrefcount(obj_1) == 0


def test_unreferencing_object_by_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)    
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    assert obj_2.value.value == 123


def test_accessing_expired_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)    
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    # weak-ref expires when the pointed object is deleted
    del obj_1
    db0.clear_cache()
    db0.commit()
    # exception due to expired weak-ref
    with pytest.raises(db0.ReferenceError):
        assert obj_2.value.value == 123


def test_uuid_of_expired_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    uuid_1 = db0.uuid(obj_1)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    # weak-ref expires when the pointed object is deleted
    del obj_1
    db0.clear_cache()
    db0.commit()
    # even though the weak-ref is expired, the uuid of the object should be still retrievable
    uuid_2 = db0.uuid(obj_2.value)
    assert uuid_1 == uuid_2
