# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import time
import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoScopedSingleton, MemoScopedClass
from .conftest import DB0_DIR
from datetime import datetime


def rand_string(str_len):
    import random
    import string
    return ''.join(random.choice(string.ascii_letters) for i in range(str_len))


def test_new_object_inside_atomic_operation(db0_fixture):
    # this is to create a new class in dbzero
    MemoTestClass(123)
    with db0.atomic() as atomic:
        object_2 = MemoTestClass(951)
        assert object_2.value == 951
        atomic.cancel()
    

def test_new_type_reverted_from_atomic_operation(db0_no_autocommit):    
    with db0.atomic() as atomic:
        # since MemoTestClass is used for the 1st time its type will be created
        object_1 = MemoTestClass(951)        
        atomic.cancel()
    # MemoTestClass type created here again (after atomic cancel)
    object_2 = MemoTestClass(123)
    assert object_2.value == 123
    
    
def test_query_after_atomic_cancel(db0_fixture):
    # this is to create a new class in dbzero
    object_1 = MemoTestClass(123)    
    with db0.atomic() as atomic:
        object_2 = MemoTestClass(951)
        atomic.cancel()
    assert object_1.value == 123


def test_read_after_atomic_create(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic():
        object_2 = MemoTestClass(951)
    assert object_1.value == 123
    assert object_2.value == 951


def test_read_after_atomic_update(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic():
        object_1.value = 951
    assert object_1.value == 951


def test_reading_after_atomic_cancel(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic() as atomic:
        object_1.value = 951
        atomic.cancel()
    assert object_1.value == 123
    
    
def test_assign_tags_inside_atomic_operation(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic():
        db0.tags(object_1).add("tag1")
        assert len(list(db0.find("tag1"))) == 1
    
    assert len(list(db0.find("tag1"))) == 1
    
    
def test_assign_and_revert_tags_inside_atomic_operation(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic() as atomic:
        db0.tags(object_1).add("tag1")
        assert len(list(db0.find("tag1"))) == 1        
        atomic.cancel()
        
    assert len(list(db0.find("tag1"))) == 0
    
    
def test_atomic_list_update(db0_fixture):
    object_1 = MemoTestClass([0])
    with db0.atomic():
        object_1.value.append(1)
        object_1.value.append(2)
        object_1.value.append(3)
    assert object_1.value == [0, 1, 2, 3]
    
    
def test_atomic_revert_list_update(db0_fixture):
    object_1 = MemoTestClass([1,2])
    with db0.atomic() as atomic:
        object_1.value.append(3)
        object_1.value.append(4)
        object_1.value.append(5)
        atomic.cancel()
    
    assert object_1.value == [1, 2]
    

def test_atomic_set_update_issue(db0_fixture):
    object_1 = MemoTestClass(set())
    object_1.value.add(0)
    with db0.atomic():
        object_1.value.add(1)


def test_atomic_set_update_issue_2(db0_fixture):
    object_1 = MemoTestClass(set([0]))
    with db0.atomic():
        object_1.value.add(1)

    
def test_atomic_set_update(db0_fixture):
    object_1 = MemoTestClass(set([0]))
    with db0.atomic():
        object_1.value.add(1)
        object_1.value.add(3)
    assert set(object_1.value) == set([0, 1, 3])
    
    
def test_atomic_revert_set_update(db0_fixture):
    object_1 = MemoTestClass(set([1, 2, 4]))
    with db0.atomic() as atomic:
        object_1.value.add(3)
        object_1.value.add(5)
        atomic.cancel()
    assert set(object_1.value) == set([1, 2, 4])
    
    
def test_atomic_dict_update(db0_fixture):
    object_1 = MemoTestClass({0:"a", 1:"b"})
    with db0.atomic():
        object_1.value[2] = "c"
        object_1.value[9] = "d"
    
    assert dict(object_1.value) == {0:"a", 1:"b", 2:"c", 9:"d"}
    
    
def test_atomic_revert_dict_update(db0_fixture):
    object_1 = MemoTestClass({0:"a", 1:"b"})
    with db0.atomic() as atomic:
        object_1.value[2] = "c"
        object_1.value[9] = "d"
        atomic.cancel()    
    assert dict(object_1.value) == {0:"a", 1:"b"}


def test_atomic_tags_assign(db0_no_autocommit, memo_tags):
    l1 = len(list(db0.find("tag1")))
    with db0.atomic():
        for _ in range(5):
            object = MemoTestClass(999)
            db0.tags(object).add("tag1")

    assert len(list(db0.find("tag1"))) == l1 + 5
    
    
def test_atomic_tags_revert_assign(db0_fixture, memo_tags):
    l1 = len(list(db0.find("tag1")))
    with db0.atomic() as atomic:
        for _ in range(5):
            object = MemoTestClass(999)
            db0.tags(object).add("tag1")
        atomic.cancel()

    assert len(list(db0.find("tag1"))) == l1
    
    
def test_atomic_index_add(db0_fixture):
    index = db0.index()
    with db0.atomic():
        index.add(1, MemoTestClass(100))
        index.add(2, MemoTestClass(200))
    # validate with the range query
    values = set([x.value for x in index.select(0, 100)])
    assert values == set([100, 200])


def test_atomic_index_create(db0_fixture):
    obj = MemoTestClass(None)
    with db0.atomic():
        obj.value = db0.index()    
        obj.value.add(None, MemoTestClass(100))    
    assert len(list(obj.value.select(None, 100, null_first=True))) == 1


def test_atomic_index_add_with_transaction(db0_fixture):
    prefix = db0.get_current_prefix()
    root = MemoTestSingleton(db0.index())
    index = root.value
    with db0.atomic():
        index.add(1, MemoTestClass(100))
        index.add(2, MemoTestClass(200))
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix.name, "r")
    # validate with the range query
    index = MemoTestSingleton().value
    values = set([x.value for x in index.select(0, 100)])
    assert values == set([100, 200])
    

@pytest.mark.parametrize("flush", [True, False])
def test_atomic_index_revert_add(db0_fixture, flush):
    index = db0.index()
    index.add(1, MemoTestClass(200))
    with db0.atomic() as atomic:
        index.add(2, MemoTestClass(100))
        index.add(3, MemoTestClass(300))
        if flush:
            index.flush()
        atomic.cancel()
    # validate with the range query
    values = set([x.value for x in index.select(0, 100)])
    assert values == set([200])


@pytest.mark.parametrize("flush", [True, False])
def test_atomic_index_remove(db0_fixture, flush):
    index = db0.index()    
    obj_1 = MemoTestClass(999)    
    index.add(1, obj_1)
    if flush:
        index.flush()
    with db0.atomic():
        index.remove(1, obj_1)    
    assert len(index) == 0


@pytest.mark.parametrize("flush", [True, False])
def test_atomic_index_revert_remove(db0_fixture, flush):
    index = db0.index()
    obj_1 = MemoTestClass(999)    
    index.add(1, obj_1)
    if flush:    
        index.flush()
    with db0.atomic() as atomic:
        index.remove(1, obj_1) 
        atomic.cancel()   
    assert len(index) == 1


def test_transaction_number_not_affected_by_atomic(db0_fixture):    
    state_num = db0.get_state_num()
    with db0.atomic():
        for _ in range(5):
            object = MemoTestClass(999)
            db0.tags(object).add("tag1")
    assert state_num == db0.get_state_num()


def test_atomic_operation_merged_into_current_transaction(db0_fixture):
    prefix = db0.get_current_prefix()
    with db0.atomic():
        for _ in range(5):
            object = MemoTestClass(999)
            db0.tags(object).add("tag1")
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    # open db0 as read-only
    db0.open(prefix.name, "r")
    # results of the atomic update should be available in the transaction
    assert len(list(db0.find("tag1"))) == 5


def test_atomic_operation_results_accessible_from_snapshot(db0_fixture):
    with db0.atomic():
        for _ in range(5):
            object = MemoTestClass(999)
            db0.tags(object).add("tag1")    
    db0.commit()
    snap = db0.snapshot()
    for _ in range(3):
        object = MemoTestClass(999)
        db0.tags(object).add("tag1")
    
    # results of the atomic update should be available in the transaction
    assert len(list(snap.find("tag1"))) == 5


def test_atomic_index_as_member(db0_fixture):
    root = MemoTestSingleton({})
    with db0.atomic():
        root.value["x"] = MemoTestClass(db0.index())     
        # add to index
        root.value["x"].value.add(None, MemoTestClass(100))
    
    # check if element was added to index
    root = MemoTestSingleton()
    assert len(list(root.value["x"].value.select(None, 100, null_first=True))) == 1


def test_atomic_with_multiple_prefixes(db0_fixture):
    prefix = "test-data"
    obj = MemoScopedClass(None, prefix=prefix)    
    with db0.atomic():
        obj.value = db0.index()
        obj.value.add(None, MemoScopedClass(100, prefix=prefix))
    
    assert len(list(obj.value.select(None, 100, null_first=True))) == 1
    

def test_multiple_atomic_index_updates_with_multiple_prefixes_issue_1(db0_fixture):
    prefix = "test-data"
    obj = MemoScopedClass(None, prefix=prefix)    
    with db0.atomic():
        obj.value = db0.index()
        obj.value.add(1, MemoScopedClass(None, prefix=prefix))
    
    with db0.atomic():
        obj.value.add(2, MemoScopedClass(None, prefix=prefix))        

    with db0.atomic():
        pass
    
    assert len(list(obj.value.select(None, 10, null_first=True))) == 2

    
def test_multiple_atomic_index_updates_with_multiple_prefixes_issue_2(db0_fixture):
    prefix = "test-data"
    obj = MemoScopedClass(None, prefix=prefix)
    index = 0
    with db0.atomic():
        obj.value = db0.index()
        for _ in range(3):
            obj.value.add(index, MemoScopedClass(None, prefix=prefix))
            index += 1
    
    with db0.atomic():
        for _ in range(3):
            obj.value.add(index, MemoScopedClass(None, prefix=prefix))
            index += 1
    
    with db0.atomic():
        for _ in range(3):
            obj.value.add(index, MemoScopedClass(None, prefix=prefix))
            index += 1
    
    assert len(list(obj.value.select(None, index, null_first=True))) == 9


def test_atomic_operation_auto_canceled_on_exception(db0_fixture):
    object_1 = MemoTestClass(123)
    try:
        with db0.atomic() as atomic:
            object_1.value = 951
            raise Exception("Test exception")
    except Exception:
        pass
    assert object_1.value == 123
    
    
def test_atomic_context_reraises_exception(db0_fixture):
    object_1 = MemoTestClass(123)
    try:
        with db0.atomic() as atomic:
            object_1.value = 951
            raise RuntimeError("Test exception")
    except RuntimeError as e:
        assert str(e) == "Test exception"
    
    
@pytest.mark.stress_test
def test_atomic_stress_test_1(db0_no_autocommit):
    count = 0
    buf = db0.list()
    for _ in range(250):
        with db0.atomic():
            for _ in range(100):
                buf.append(MemoTestClass(rand_string(4096)))
        count += 1
        print(f"Atomic operations completed: {count}")


def test_atomic_deletion(db0_fixture):
    obj = MemoTestClass(MemoTestClass(123))    
    dep_uuid = db0.uuid(obj.value)
    # drop related object as atomic
    with db0.atomic():
        obj.value = None    
    db0.commit()
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)

    
def test_atomic_deletion_issue_1(db0_fixture):
    """
    This test was failing due to incorrect implementation of AtomicContext.exit() - 
    the method was not releasing references to associated objects
    """
    obj = MemoTestClass(MemoTestClass(123))
    dep_uuid = db0.uuid(obj.value)
    # drop related object as atomic
    with db0.atomic() as atomic:
        obj.value = None    
    db0.commit()
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)


def test_reverting_atomic_deletion(db0_fixture):
    obj = MemoTestClass(MemoTestClass(123))    
    dep_uuid = db0.uuid(obj.value)
    # drop related object as atomic without completing the operation
    try:
        with db0.atomic():
            obj.value = None
            # NOTE: object not dropped yet because it's referenced from the atomic context
            raise Exception("Test exception")
    except Exception:
        pass
    
    # drop/assign should be reverted by here    
    db0.commit()
    db0.fetch(dep_uuid)
    assert db0.uuid(obj.value) == dep_uuid


def test_reverting_atomic_free(db0_fixture):
    obj = MemoTestClass([1, 2, 3])
    obj_uuid = db0.uuid(obj)
    count_1 = db0.get_cache_stats()["deferred_free_count"]
    try:
        with db0.atomic():
            # NOTE: list.append may internally perform a free operation to reallocate list            
            for i in range(1000):
                obj.value.append(i)
            assert db0.get_cache_stats()["deferred_free_count"] > count_1
            raise Exception("Test exception")
    except Exception:
        pass
    
    # free/deferred free should be reverted by here
    assert db0.get_cache_stats()["deferred_free_count"] == count_1
    assert list(obj.value) == [1, 2, 3]
    db0.commit()
    assert list(db0.fetch(obj_uuid).value) == [1, 2, 3]


def test_atomic_infinite_loop_issue_1(db0_no_autocommit):
    """
    This test was getting into an infinite loop on RC_LimitedStringPool::get() 
    but only in the 'release' build, even after empty atomic begin / exit context
    FIXED: added Workspace::preAtomic call and fixed Object::commit implementation
    """            
    for i in range(2):
        obj = MemoTestClass(0)
        db0.tags(obj).add("tag1")
        if i % 2 == 0:
            db0.tags(obj).add("tag2")
    
    with db0.atomic():
        pass
    
    assert len(list(db0.find("tag1"))) > 0


def test_atomic_infinite_loop_issue_2(db0_no_autocommit):
    """
    This test was getting into an infinite loop on RC_LimitedStringPool::get() 
    but only in the 'release' build, even after empty atomic begin / exit context
    NOTE: blocking Object::detach from AtomicContext seems to solve the problem
    NOTE: it looks like data is generated correctly but the application's state gets corrupted after 'atomic'
    """
    for i in range(2):
        obj = MemoTestClass(0)
        db0.tags(obj).add("tag1")
        if i % 2 == 0:
            db0.tags(obj).add("tag2")
    
    with db0.atomic():
        for _ in range(2):
            object = MemoTestClass(999)
            db0.tags(object).add("tag1")
    
    assert len(list(db0.find("tag1"))) > 0


def test_atomic_context_does_not_increase_state_num(db0_fixture):
    state_1 = db0.get_state_num()
    with db0.atomic():
        assert db0.get_state_num() == state_1
