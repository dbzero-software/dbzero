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

# FIXME: __exit__ needs to be fixed
# def test_snapshot_can_be_used_as_context_manager(db0_fixture):
#     object_1 = MemoTestSingleton(123)
#     uuid = db0.uuid(object_1)
#     del object_1
#     with db0.snapshot() as snap:
#         db0.commit()
#         object_1 = db0.fetch(uuid)
#         object_1.value = 456
#         object_1 = snap.fetch(uuid)
#         assert object_1.value == 123
    
#     # make sure snapshot is closed
#     with pytest.raises(Exception):
#         snap.fetch(uuid)


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


def test_find_in_snapshot(db0_fixture):
    for i in range(3):
        object_1 = MemoTestClass(i)
        db0.tags(object_1).add("some-tag")
    snap = db0.snapshot()
    db0.commit()
    for i in range(3):
        object_1 = MemoTestClass(i + 3)
        db0.tags(object_1).add("some-tag")

    assert set([x.value for x in snap.find("some-tag")]) == set([0, 1, 2])

    
def test_tag_query_from_two_snapshots(db0_fixture):
    for i in range(3):
        object_1 = MemoTestClass(i)
        db0.tags(object_1).add("some-tag")
    snap1 = db0.snapshot()
    db0.commit()    
    for i in range(3):
        object_1 = MemoTestClass(i + 3)
        db0.tags(object_1).add("some-tag")
    snap2 = db0.snapshot()
    db0.commit()
    for i in range(5):
        object_1 = MemoTestClass(i + 6)
        db0.tags(object_1).add("some-tag")

    # run same queries on different snapshots
    result_1 = set([x.value for x in snap1.find("some-tag")])
    result_2 = set([x.value for x in snap2.find("some-tag")])
    assert result_1 == set([0, 1, 2])
    assert result_2 == set([0, 1, 2, 3, 4, 5])
    
    
def test_find_can_join_results_from_two_snapshots(db0_fixture):
    for i in range(3):
        object_1 = MemoTestClass(i)
        db0.tags(object_1).add("some-tag")
    snap1 = db0.snapshot()
    db0.commit()    
    for i in range(3):
        object_1 = MemoTestClass(i + 3)
        db0.tags(object_1).add("some-tag")
    snap2 = db0.snapshot()
    db0.commit()
    for i in range(5):
        object_1 = MemoTestClass(i + 6)
        db0.tags(object_1).add("some-tag")

    # run same queries on different snapshots
    query_1 = snap1.find("some-tag")
    query_2 = snap2.find("some-tag")
    
    result = set([x.value for x in db0.find(query_1, query_2)])
    assert result == set([0, 1, 2])
    
    
def test_snapshot_delta_queries(db0_fixture):
    objects = []
    for i in range(3):
        object_1 = MemoTestClass(i)
        objects.append(object_1)
        db0.tags(object_1).add("some-tag")
    snap1 = db0.snapshot()
    db0.commit()
    for i in range(3):
        object_1 = MemoTestClass(i + 3)
        db0.tags(object_1).add("some-tag")
    db0.tags(objects[1]).remove("some-tag")
    snap2 = db0.snapshot()
    db0.commit()
    for i in range(5):
        object_1 = MemoTestClass(i + 6)
        db0.tags(object_1).add("some-tag")

    # run same queries on different snapshots
    query_1 = snap1.find("some-tag")
    query_2 = snap2.find("some-tag")
    
    created = set([x.value for x in snap2.find(query_2, db0.no(query_1))])
    deleted = set([x.value for x in snap1.find(query_1, db0.no(query_2))])
    
    assert created == set([3, 4, 5])
    assert deleted == set([1])
    

def test_snapshot_can_be_taken_with_state_num(db0_fixture):
    for i in range(3):
        object_1 = MemoTestClass(i)    
        db0.tags(object_1).add("some-tag")
    old_state_id = db0.get_state_num()
    db0.commit()
    for i in range(3):
        object_1 = MemoTestClass(i + 3)
        db0.tags(object_1).add("some-tag")        
    db0.commit()
    snap = db0.snapshot(old_state_id)
    values = [x.value for x in snap.find("some-tag")]
    assert set(values) == set([0, 1, 2])
