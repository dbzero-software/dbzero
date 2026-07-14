# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoDataPxSingleton
from .conftest import DB0_DIR


def test_snapshot_can_fetch_object_by_id(db0_fixture):
    object_1 = MemoTestSingleton(123)
    uuid = db0.uuid(object_1)
    prefix_name = db0.get_prefix_of(object_1).name
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
    db0.commit()
    snap = db0.snapshot()
    
    object_1 = db0.fetch(uuid)
    object_1.value = 456
    # read past state from the snapshot
    object_1 = snap.fetch(uuid)
    assert object_1.value == 123


def test_snapshot_can_fetch_memo_singleton(db0_fixture):
    object_1 = MemoTestSingleton(123)
    prefix_name = db0.get_prefix_of(object_1).name
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


def test_snapshot_can_be_used_as_context_manager(db0_fixture):
    object_1 = MemoTestSingleton(123)
    uuid = db0.uuid(object_1)
    del object_1
    db0.commit()
    with db0.snapshot() as snap:        
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
    db0.commit()
    with db0.snapshot() as snap:        
        object_1 = db0.fetch(uuid)
        # replace inner object with simple value
        object_1.value = 456
        object_1 = snap.fetch(uuid)
        assert object_1.value.value == 123


def test_find_in_snapshot(db0_fixture):
    for i in range(3):
        object_1 = MemoTestClass(i)
        db0.tags(object_1).add("some-tag")    
    db0.commit()
    snap = db0.snapshot()
    for i in range(3):
        object_1 = MemoTestClass(i + 3)
        db0.tags(object_1).add("some-tag")

    assert set([x.value for x in snap.find("some-tag")]) == set([0, 1, 2])

    
def test_tag_query_from_two_snapshots(db0_fixture):
    for i in range(3):
        object_1 = MemoTestClass(i)
        db0.tags(object_1).add("some-tag")
    state_num_1 = db0.get_state_num()
    db0.commit()
    snap1 = db0.snapshot(state_num_1)
    for i in range(3):
        object_1 = MemoTestClass(i + 3)
        db0.tags(object_1).add("some-tag")
    state_num_2 = db0.get_state_num()
    db0.commit()
    snap2 = db0.snapshot(state_num_2)
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
    db0.commit()
    snap1 = db0.snapshot()
    for i in range(3):
        object_1 = MemoTestClass(i + 3)
        db0.tags(object_1).add("some-tag")    
    db0.commit()
    snap2 = db0.snapshot()
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
    state_num_1 = db0.get_state_num()
    db0.commit()
    snap1 = db0.snapshot(state_num_1)
    for i in range(3):
        object_1 = MemoTestClass(i + 3)
        db0.tags(object_1).add("some-tag")
    db0.tags(objects[1]).remove("some-tag")    
    state_num_2 = db0.get_state_num()
    db0.commit()
    snap2 = db0.snapshot(state_num_2)
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


def test_tag_query_over_snapshot(db0_fixture, memo_tags):    
    db0.commit()
    snap = db0.snapshot()
    # add more tags in a new transaction
    db0.tags(MemoTestClass(10)).add("tag1")
    
    assert len(list(db0.find("tag1"))) == 11
    assert len(list(snap.find("tag1"))) == 10


def test_retrieving_object_dependencies_from_snapshot(db0_fixture):
    obj_1 = MemoTestClass(9123)
    obj_2 = MemoTestClass(obj_1)
    # assign tags to persist objects
    db0.tags(obj_1, obj_2).add("temp")                       
    state_num_1 = db0.get_state_num()
    db0.commit()
    snap_1 = db0.snapshot(state_num_1)
    obj_3 = MemoTestClass(91237123)
    obj_2.value = obj_3
    state_num_2 = db0.get_state_num()
    db0.commit()    
    snap_2 = db0.snapshot(state_num_2)
    # retrieve related object from snapshot
    obj = snap_1.fetch(db0.uuid(obj_2))
    assert obj.value.value == 9123
    obj = snap_2.fetch(db0.uuid(obj_2))
    assert obj.value.value == 91237123


def test_retrieving_snapshot_specific_object_version(db0_fixture):
    obj_1 = MemoTestClass(9123)
    obj_2 = MemoTestClass(obj_1)
    # assign tags to persist objects
    db0.tags(obj_1, obj_2).add("temp")
    db0.commit()
    snap = db0.snapshot()
    obj_1.value = 1234
    db0.commit()
    # retrieve related object from snapshot
    obj = snap.fetch(db0.uuid(obj_2))
    assert obj.value.value == 9123


def test_snapshot_find_query(db0_fixture):
    for i in range(10):        
        db0.tags(MemoTestClass(i)).add(["tag1", "tag2"])
    db0.commit()
    with db0.snapshot() as snap:
        query = snap.find(("tag1", "tag2"))
        assert len(list(query)) == 10


def test_snapshot_mutation_attempt_should_raise_exception(db0_fixture):
    for i in range(10):
        obj = MemoTestClass(i)
        db0.tags(obj).add(["tag1", "tag2"])
    db0.commit()
    # run query over a snapshot and try updating it
    count = 0
    with db0.snapshot() as snap:
        for obj in snap.find(("tag1", "tag2")):
            with pytest.raises (Exception):
                db0.tags(obj).remove("tag1")
            count += 1
    
    assert count == 10


def test_snapshot_get_state_num_of_prefix(db0_fixture, memo_tags):
    prefix = db0.get_current_prefix().name
    state_num = db0.get_state_num(prefix)
    db0.commit()
    with db0.snapshot() as snap:
        # must be same as the last fully commited state
        assert snap.get_state_num(prefix) == state_num


def test_get_frozen_head_snapshot(db0_fixture):
    with db0.snapshot() as head_1:
        with db0.snapshot(frozen=True) as head_2:
            assert head_1.get_state_num() == head_2.get_state_num()            
    
    
def test_exception_when_attempting_to_access_frozen_head_snapshot(db0_fixture):
    """
    The "frozen" snapshot cannot be taken because it's not initialized
    """
    with pytest.raises(Exception):
        with db0.snapshot(frozen=True) as head:
            pass
    
    
def test_auto_open_fixture_in_head_snapshot(db0_fixture):
    px_name = db0.get_current_prefix().name
    # create root object on the data prefix
    root = MemoDataPxSingleton(123)
    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")    
    # take the head snapshot before data-px has been opened
    with db0.snapshot():
        with db0.snapshot(frozen=True) as head_2:
            # on fetch attempt, the prefix should be auto-opened as head
            root = head_2.fetch(MemoDataPxSingleton)
            assert root.value == 123


def test_out_of_context_snapshot_query(db0_fixture):
    obj_list = [MemoTestClass(123) for _ in range(100)]
    for obj in obj_list:
        db0.tags(obj).add("tag1")    
    state_1 = db0.get_state_num()
    db0.commit()
    obj_2 = MemoTestClass(123)
    db0.tags(obj_2).add("tag1")
    db0.commit()
    with db0.snapshot(state_1) as snap:
        query = snap.find("tag1")
    
    # NOTE: an exception raised because query executed out of the context
    with pytest.raises(Exception):
        assert len(query) == 100
    
    
def test_snapshot_scope_bound_to_query(db0_fixture):
    obj_list = [MemoTestClass(123) for _ in range(100)]
    for obj in obj_list:
        db0.tags(obj).add("tag1")    
    state_1 = db0.get_state_num()
    db0.commit()
    obj_2 = MemoTestClass(123)
    db0.tags(obj_2).add("tag1")
    db0.commit()
    query = db0.snapshot(state_1).find("tag1")
    # snapshot's scope should be bound with the scoped of the query
    assert len(query) == 100


def test_out_of_context_object_access(db0_fixture):
    obj_list = [MemoTestClass(123) for _ in range(100)]
    # add tags to persist objects
    db0.tags(*obj_list).add("temp")        
    state_1 = db0.get_state_num()
    db0.commit()
    for obj in obj_list:
        obj.value = 999
    db0.commit()
    with db0.snapshot(state_1) as snap:
        # re-fetch objects from the snapshot
        obj_list = [snap.fetch(db0.uuid(obj)) for obj in obj_list]        
    
    # NOTE: an exception raised because query executed out of the context
    with pytest.raises(Exception):
        for obj in obj_list:
            assert obj.value == 123        


def test_check_tags_assignment_in_transactions(db0_fixture):
    obj = MemoTestClass(123)
    db0.tags(obj).add("tag1")
    db0.commit()
    state_1 = db0.get_state_num(finalized=True)
    db0.tags(obj).add("tag2")
    db0.commit()
    state_2 = db0.get_state_num(finalized=True)
    
    # NOTE: snapshots must be retained for the objects to be accessible
    snap_1 = db0.snapshot(state_1)
    snap_2 = db0.snapshot(state_2)
    
    # compare tags applied to 2 versions of the same object
    ver_1 = snap_1.fetch(db0.uuid(obj))
    ver_2 = snap_2.fetch(db0.uuid(obj))
    
    # check number of tags assigned to the object (might be from a snapshot)
    def num_tags(obj, tag):
        return len(db0.get_snapshot_of(obj).find(MemoTestClass, tag, obj))

    assert num_tags(ver_1, "tag1") == 1
    assert num_tags(ver_1, "tag2") == 0
    
    assert num_tags(ver_2, "tag1") == 1
    assert num_tags(ver_2, "tag2") == 1
    
    
def test_snapshot_in_operator(db0_fixture):
    px_name = "some-other-prefix"
    snap_0 = db0.snapshot()
    assert db0.get_current_prefix().name in snap_0
    assert px_name not in snap_0
