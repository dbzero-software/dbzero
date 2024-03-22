import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


def test_snapshot_can_fetch_object_by_id(db0_fixture):
    object_1 = MemoTestSingleton(123)
    uuid = db0.uuid(object_1)
    prefix_name = db0.get_prefix(object_1)
    del object_1
    # commit data and close db0
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    # open db0 as read-only
    db0.open(prefix_name, "r")    
    # take snapshot of the current state
    snap = db0.snapshot()
    # fetch object from the snapshot
    object_1 = snap.fetch(uuid)
    assert object_1.value == 123


def test_snapshot_can_access_data_from_past_transaction(db0_fixture):
    object_1 = MemoTestSingleton(123)    
    uuid = db0.uuid(object_1)
    del object_1
    # take snapshot before commit
    snap = db0.snapshot()    
    db0.commit()
    
    object_1 = db0.fetch(uuid)
    object_1.value = 456
    # read past state from the snapshot
    object_1 = snap.fetch(uuid)
    assert object_1.value == 123


def test_snapshot_can_fetch_memo_singleton(db0_fixture):
    object_1 = MemoTestSingleton(123)
    prefix_name = db0.get_prefix(object_1)
    del object_1
    # commit data and close db0
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    # open db0 as read-only
    db0.open(prefix_name, "r")    
    # take snapshot of the current state
    snap = db0.snapshot()
    # fetch singleton from the snapshot
    object_1 = snap.fetch(MemoTestSingleton)
    assert object_1.value == 123


def test_snapshot_can_be_closed(db0_fixture):
    object_1 = MemoTestSingleton(123)
    uuid = db0.uuid(object_1)
    del object_1
    # take snapshot before commit
    snap = db0.snapshot()    
    db0.commit()
    
    object_1 = db0.fetch(uuid)
    object_1.value = 456
    snap.close()
    with pytest.raises(Exception):
        snap.fetch(uuid)


def test_snapshot_can_be_used_as_contex_manager(db0_fixture):
    object_1 = MemoTestSingleton(123)    
    uuid = db0.uuid(object_1)
    del object_1
    with db0.snapshot() as snap:
        db0.commit()
        object_1 = db0.fetch(uuid)
        object_1.value = 456
        object_1 = snap.fetch(uuid)
        assert object_1.value == 123
    
    # make sure snapshot is closed    
    with pytest.raises(Exception):
        snap.fetch(uuid)


def test_snapshot_with_nested_objects(db0_fixture):
    # create object with inner object
    object_1 = MemoTestSingleton(MemoTestClass(123))
    uuid = db0.uuid(object_1)
    del object_1
    with db0.snapshot() as snap:
        db0.commit()
        object_1 = db0.fetch(uuid)
        # replace inner object with simple value
        object_1.value = 456
        object_1 = snap.fetch(uuid)
        assert object_1.value.value == 123
