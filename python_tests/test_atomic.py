# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import time
import gc
import random
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


@pytest.mark.stress_test
def test_nested_atomic_stress_test_1(db0_no_autocommit):
    rng = random.Random(0xDB0A70)
    buf = db0.list()
    state = MemoTestClass({"counter": 0})
    expected_count = 0

    def make_payload(outer_index, group_index, level, item_index):
        header = f"{outer_index}:{group_index}:{level}:{item_index}:"
        return header + rand_string(4096 - len(header))

    def append_items(outer_index, group_index, level, item_count):
        for item_index in range(item_count):
            buf.append(MemoTestClass(make_payload(outer_index, group_index, level, item_index)))
        state.value["counter"] = state.value["counter"] + item_count
        return item_count

    def run_nested_block(outer_index, group_index, level):
        mode = rng.choices(["commit", "cancel", "exception"], weights=[6, 2, 2], k=1)[0]
        max_depth = rng.randint(2, 4)
        item_count = rng.randint(1, 4)
        committed_count = 0

        try:
            with db0.atomic() as atomic:
                committed_count += append_items(outer_index, group_index, level, item_count)

                if level < max_depth and rng.random() < 0.75:
                    committed_count += run_nested_block(outer_index, group_index, level + 1)

                if mode == "cancel":
                    atomic.cancel()
                    return 0

                if mode == "exception":
                    raise RuntimeError("nested atomic rollback")

            return committed_count
        except RuntimeError:
            return 0

    for outer_index in range(250):
        outer_committed_count = 0
        with db0.atomic():
            outer_committed_count += append_items(outer_index, -1, 0, 40)

            for group_index in range(10):
                outer_committed_count += run_nested_block(outer_index, group_index, 1)

        expected_count += outer_committed_count
        print(f"Nested atomic operations completed: {outer_index + 1}")

    assert len(buf) == expected_count
    assert state.value["counter"] == expected_count


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


def test_nested_atomic_cancel_reverts_only_nested_changes(db0_fixture):
    object_1 = MemoTestClass(1)
    with db0.atomic():
        object_1.value += 10
        try:
            with db0.atomic():
                object_1.value += 20
                raise RuntimeError("nested failure")
        except RuntimeError:
            pass

    assert object_1.value == 11


def test_nested_atomic_success_merges_into_parent(db0_fixture):
    object_1 = MemoTestClass(1)
    with db0.atomic():
        object_1.value += 10
        with db0.atomic():
            object_1.value += 20

    assert object_1.value == 31


def test_deep_nested_atomic_cancel_reverts_top_only(db0_fixture):
    object_1 = MemoTestClass(1)
    with db0.atomic():
        object_1.value += 10
        with db0.atomic():
            object_1.value += 20
            with db0.atomic() as atomic:
                object_1.value += 30
                atomic.cancel()

    assert object_1.value == 31


def test_parent_cancel_reverts_successful_nested_atomic(db0_fixture):
    object_1 = MemoTestClass(1)
    with db0.atomic() as atomic:
        object_1.value += 10
        with db0.atomic():
            object_1.value += 20
        atomic.cancel()

    assert object_1.value == 1


def test_nested_atomic_list_cancel_reverts_only_nested_changes(db0_fixture):
    object_1 = MemoTestClass([1])
    with db0.atomic():
        object_1.value.append(2)
        try:
            with db0.atomic():
                object_1.value.append(3)
                raise RuntimeError("nested failure")
        except RuntimeError:
            pass
        object_1.value.append(4)

    assert list(object_1.value) == [1, 2, 4]


def test_nested_atomic_can_begin_after_grandchild_rollback_with_list_update(db0_fixture):
    items = db0.list()
    root = MemoTestClass({"items": items, "counter": 0})

    with db0.atomic():
        root.value["counter"] = 1
        committed_child = MemoTestClass("child")
        items.append(committed_child)

        try:
            with db0.atomic():
                rolled_child = MemoTestClass("rolled")
                root.value["counter"] = 999
                items.append(rolled_child)

                with db0.atomic():
                    root.value["counter"] = 1000
                    raise RuntimeError("grandchild rollback")
        except RuntimeError:
            pass

        with db0.atomic():
            root.value["counter"] = 2

    assert root.value["counter"] == 2
    assert [obj.value for obj in items] == ["child"]
    root = items = committed_child = None
    gc.collect()


def test_nested_atomic_rollback_of_new_tagged_object_is_gc_safe(db0_fixture):
    items = db0.list()

    with db0.atomic():
        committed_child = MemoTestClass("child")
        items.append(committed_child)

        try:
            with db0.atomic():
                rolled_child = MemoTestClass("rolled")
                items.append(rolled_child)
                db0.tags(rolled_child).add("nested-rolled")

                with db0.atomic():
                    raise RuntimeError("grandchild rollback")
        except RuntimeError:
            pass

    assert [obj.value for obj in items] == ["child"]
    assert len(list(db0.find("nested-rolled"))) == 0
    items = committed_child = rolled_child = None
    gc.collect()


def test_nested_atomic_cancel_reverts_index_add_without_corrupting_index(db0_fixture):
    index = db0.index()
    committed = MemoTestClass("committed")
    canceled = None

    with db0.atomic():
        index.add(1, committed)

        with db0.atomic() as atomic:
            canceled = MemoTestClass("canceled")
            index.add(2, canceled)
            atomic.cancel()
            canceled = None

        assert [obj.value for obj in index.select(0, 10)] == ["committed"]

    assert [obj.value for obj in index.select(0, 10)] == ["committed"]
    index = committed = canceled = None
    gc.collect()


def test_nested_atomic_rollback_preserves_parent_list_slab_metadata(db0_fixture):
    items = db0.list()

    with db0.atomic():
        for i in range(20):
            items.append(f"parent-before-{i}")

        try:
            with db0.atomic():
                for i in range(20):
                    items.append(f"child-rollback-{i}")
                raise RuntimeError("rollback child list writes")
        except RuntimeError:
            pass

        for i in range(20):
            items.append(f"parent-after-{i}")

    assert list(items) == [
        *(f"parent-before-{i}" for i in range(20)),
        *(f"parent-after-{i}" for i in range(20)),
    ]
    items = None
    gc.collect()


def test_deep_nested_atomic_mixed_commit_and_rollback(db0_fixture):
    items = db0.list()
    index = db0.index()
    keep = MemoTestClass("keep")
    drop = MemoTestClass("drop")
    root = MemoTestClass({"drop": drop, "counter": 0})
    drop = None

    items.append(keep)
    index.add(1, keep)
    db0.tags(keep).add("nested-keep")

    with db0.atomic():
        root.value["counter"] = 1
        committed_child = MemoTestClass("child-commit")
        items.append(committed_child)
        index.add(2, committed_child)
        db0.tags(committed_child).add("nested-child-commit")

        try:
            with db0.atomic():
                nonlocal_marker = MemoTestClass("child-rollback")
                root.value["counter"] = 999
                root.value["drop"] = None
                items.append(nonlocal_marker)
                index.add(3, nonlocal_marker)
                db0.tags(nonlocal_marker).add("nested-child-rollback")

                with db0.atomic():
                    root.value["counter"] = 1000
                    raise RuntimeError("level 3 rollback")
        except RuntimeError:
            nonlocal_marker = None
            pass

        assert root.value["counter"] == 1
        assert [obj.value for obj in items] == ["keep", "child-commit"]
        assert {obj.value for obj in index.select(0, 10)} == {"keep", "child-commit"}
        assert len(list(db0.find("nested-child-rollback"))) == 0
        assert root.value["drop"].value == "drop"

        with db0.atomic():
            root.value["counter"] = 2
            grandchild = MemoTestClass("grandchild-commit")
            items.append(grandchild)
            index.add(4, grandchild)
            db0.tags(grandchild).add("nested-grandchild-commit")

            assert root.value["counter"] == 2
            assert [obj.value for obj in items] == [
                "keep",
                "child-commit",
                "grandchild-commit",
            ]

        root.value["drop"] = None

    assert root.value["counter"] == 2
    assert [obj.value for obj in items] == [
        "keep",
        "child-commit",
        "grandchild-commit",
    ]
    assert {obj.value for obj in index.select(0, 10)} == {
        "keep",
        "child-commit",
        "grandchild-commit",
    }
    assert len(list(db0.find("nested-keep"))) == 1
    assert len(list(db0.find("nested-child-commit"))) == 1
    assert len(list(db0.find("nested-grandchild-commit"))) == 1
    assert len(list(db0.find("nested-child-rollback"))) == 0
    assert root.value["drop"] is None

    root = items = index = keep = committed_child = grandchild = None
    gc.collect()
