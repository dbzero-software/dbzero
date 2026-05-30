# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import time
import gc
import random
import asyncio
import threading
import os
import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoScopedSingleton, MemoScopedClass
from .conftest import DB0_DIR
from datetime import datetime


ATOMIC_THREAD_REPRO_SKIP = (
    "atomic cross-thread/API-boundary repro kept disabled: observed non-owner "
    "thread mutations can enter an active atomic scope or corrupt rollback state"
)
ATOMIC_ASYNC_REPRO_SKIP = (
    "atomic asyncio repro kept disabled: observed same-thread async task wait "
    "can deadlock without task-context-aware atomic ownership"
)
ATOMIC_COMMIT_REPRO_SKIP = (
    "atomic commit synchronization repro kept disabled: commit/autocommit must "
    "be serialized against active atomic operations"
)
ATOMIC_ROLLBACK_REPRO_SKIP = (
    "atomic rollback corruption repro kept disabled: observed canceled atomic "
    "tuple/type-change paths can leave stale wrapper or GC0 state"
)
ATOMIC_INDEX_NULL_KEY_REPRO_SKIP = (
    "atomic index null-key repro kept disabled: debug teardown can double-unref "
    "objects indexed under None after the index is created inside an atomic block"
)
ATOMIC_MULTI_PREFIX_REPRO_SKIP = (
    "atomic multi-prefix repro kept disabled: debug teardown aborts after atomic "
    "updates span objects from multiple prefixes"
)
ATOMIC_STRESS_REPRO_SKIP = (
    "atomic async/thread stress repro kept disabled: observed abort during "
    "teardown after mixed commits, cancels, nested atomic operations, and threads"
)
ATOMIC_INDEX_ITERATOR_REPRO_SKIP = (
    "atomic index iterator repro kept disabled: query iterators can outlive the "
    "durable lock while another thread rolls back index mutations"
)


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


def test_leaked_new_object_from_merged_atomic_block_remains_usable(db0_fixture):
    leaked = None

    with db0.atomic():
        leaked = MemoTestClass(951)
        assert leaked.value == 951

    assert leaked.value == 951


def test_leaked_new_object_from_reverted_atomic_block_is_defunct(db0_fixture):
    leaked = None

    with db0.atomic() as atomic:
        leaked = MemoTestClass(951)
        assert leaked.value == 951
        atomic.cancel()

    with pytest.raises(db0.ReferenceError):
        _ = leaked.value


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


def test_atomic_succeeds_in_synchronous_code(db0_fixture):
    obj = MemoTestClass(1)

    with db0.atomic():
        obj.value = 2

    assert obj.value == 2


async def test_atomic_raises_inside_asyncio_task(db0_fixture):
    with pytest.raises(RuntimeError, match=r"db0\.atomic is synchronous; use db0\.async_atomic\(\)"):
        with db0.atomic():
            pass


async def test_atomic_raises_before_cross_task_async_mutation_repro(db0_fixture):
    obj = MemoTestClass(0)
    atomic_started = asyncio.Event()

    async def run_atomic():
        with pytest.raises(RuntimeError, match=r"db0\.atomic is synchronous; use db0\.async_atomic\(\)"):
            with db0.atomic():
                atomic_started.set()
                obj.value = 1

    await asyncio.wait_for(run_atomic(), timeout=2)
    assert not atomic_started.is_set()
    assert obj.value == 0


def test_async_atomic_requires_running_asyncio_task(db0_fixture):
    with pytest.raises(RuntimeError, match=r"db0\.async_atomic requires a running asyncio task"):
        db0.async_atomic()


async def test_async_atomic_commits_on_normal_exit(db0_fixture):
    obj = MemoTestClass(1)

    async with db0.async_atomic():
        obj.value = 2
        await asyncio.sleep(0)

    assert obj.value == 2


async def test_async_atomic_cancels_on_exception(db0_fixture):
    obj = MemoTestClass(1)

    with pytest.raises(ValueError):
        async with db0.async_atomic():
            obj.value = 2
            await asyncio.sleep(0)
            raise ValueError("rollback")

    assert obj.value == 1


async def test_async_atomic_explicit_cancel(db0_fixture):
    obj = MemoTestClass(1)

    async with db0.async_atomic() as atomic:
        obj.value = 2
        atomic.cancel()

    assert obj.value == 1


async def test_async_atomic_rolls_back_set_mutation(db0_fixture):
    values = db0.set([1])

    async with db0.async_atomic() as atomic:
        values.add(2)
        atomic.cancel()

    assert list(values) == [1]


async def test_nested_async_atomic_same_task_preserves_rollback(db0_fixture):
    obj = MemoTestClass(1)

    async with db0.async_atomic():
        obj.value = 2
        async with db0.async_atomic() as inner:
            obj.value = 3
            inner.cancel()
        assert obj.value == 2

    assert obj.value == 2


async def test_concurrent_async_atomic_blocks_serialize_without_blocking_loop(db0_fixture):
    obj = MemoTestClass(0)
    first_entered = asyncio.Event()
    first_can_exit = asyncio.Event()
    second_entered = asyncio.Event()

    async def first():
        async with db0.async_atomic():
            obj.value = 1
            first_entered.set()
            await asyncio.wait_for(first_can_exit.wait(), timeout=2)
            obj.value = 2

    async def second():
        await asyncio.wait_for(first_entered.wait(), timeout=2)
        async with db0.async_atomic():
            second_entered.set()
            assert obj.value == 2
            obj.value = 3

    first_task = asyncio.create_task(first())
    second_task = asyncio.create_task(second())
    await asyncio.wait_for(first_entered.wait(), timeout=2)
    await asyncio.sleep(0)
    assert not second_entered.is_set()
    first_can_exit.set()
    await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=2)
    assert obj.value == 3


async def test_unguarded_async_mutation_fails_while_async_atomic_is_active(db0_fixture):
    obj = MemoTestClass(0)
    owner_started = asyncio.Event()
    owner_can_exit = asyncio.Event()

    async def owner():
        async with db0.async_atomic():
            obj.value = 1
            owner_started.set()
            await asyncio.wait_for(owner_can_exit.wait(), timeout=2)

    async def unguarded_mutation():
        await asyncio.wait_for(owner_started.wait(), timeout=2)
        with pytest.raises(RuntimeError, match=r"db0\.async_atomic"):
            obj.value = 2
        owner_can_exit.set()

    await asyncio.wait_for(asyncio.gather(owner(), unguarded_mutation()), timeout=2)
    assert obj.value == 1


async def test_unguarded_async_set_mutation_fails_while_async_atomic_is_active(db0_fixture):
    values = db0.set()
    owner_started = asyncio.Event()
    owner_can_exit = asyncio.Event()

    async def owner():
        async with db0.async_atomic():
            values.add(1)
            owner_started.set()
            await asyncio.wait_for(owner_can_exit.wait(), timeout=2)

    async def unguarded_mutation():
        await asyncio.wait_for(owner_started.wait(), timeout=2)
        with pytest.raises(RuntimeError, match=r"db0\.async_atomic"):
            values.add(2)
        owner_can_exit.set()

    await asyncio.wait_for(asyncio.gather(owner(), unguarded_mutation()), timeout=2)
    assert list(values) == [1]


async def test_commit_from_other_async_task_fails_while_async_atomic_is_active(db0_no_autocommit):
    obj = MemoTestClass(0)
    owner_started = asyncio.Event()
    owner_can_exit = asyncio.Event()

    async def owner():
        async with db0.async_atomic():
            obj.value = 1
            owner_started.set()
            await asyncio.wait_for(owner_can_exit.wait(), timeout=2)

    async def commit_task():
        await asyncio.wait_for(owner_started.wait(), timeout=2)
        with pytest.raises(RuntimeError, match=r"db0\.async_atomic"):
            db0.commit()
        owner_can_exit.set()

    await asyncio.wait_for(asyncio.gather(owner(), commit_task()), timeout=2)


async def test_thread_mutation_waits_for_async_atomic_owner(db0_fixture):
    obj = MemoTestClass(0)
    mutation_done = threading.Event()
    errors = []

    def mutate_from_thread():
        try:
            obj.value = 2
            mutation_done.set()
        except BaseException as exc:
            errors.append(exc)

    async with db0.async_atomic():
        obj.value = 1
        thread = threading.Thread(target=mutate_from_thread)
        thread.start()
        await asyncio.sleep(0.1)
        assert not mutation_done.is_set()

    thread.join(timeout=2)
    assert not thread.is_alive()
    assert errors == []
    assert mutation_done.is_set()
    assert obj.value == 2


def test_atomic_cancel_in_one_thread_must_not_revert_other_thread_mutation(db0_fixture):
    obj = MemoTestClass(0)
    atomic_started = threading.Event()
    mutation_attempting = threading.Event()
    mutation_done = threading.Event()
    errors = []

    def run_atomic():
        try:
            with db0.atomic() as atomic:
                atomic_started.set()
                assert mutation_attempting.wait(timeout=5)
                atomic.cancel()
        except BaseException as exc:
            errors.append(exc)

    def run_mutation():
        try:
            assert atomic_started.wait(timeout=5)
            mutation_attempting.set()
            obj.value = 2
            mutation_done.set()
        except BaseException as exc:
            errors.append(exc)

    atomic_thread = threading.Thread(target=run_atomic)
    mutation_thread = threading.Thread(target=run_mutation)
    atomic_thread.start()
    mutation_thread.start()
    atomic_thread.join(timeout=10)
    mutation_thread.join(timeout=10)

    assert not atomic_thread.is_alive()
    assert not mutation_thread.is_alive()
    assert errors == []
    assert mutation_done.is_set()
    assert obj.value == 2


async def test_atomic_cancel_in_one_async_task_must_not_revert_other_task_mutation(db0_fixture):
    obj = MemoTestClass(0)
    atomic_started = asyncio.Event()
    mutation_attempted = asyncio.Event()
    atomic_can_exit = asyncio.Event()

    async def run_atomic():
        async with db0.async_atomic() as atomic:
            atomic_started.set()
            await asyncio.wait_for(atomic_can_exit.wait(), timeout=5)
            atomic.cancel()

    async def run_mutation():
        await asyncio.wait_for(atomic_started.wait(), timeout=5)
        with pytest.raises(RuntimeError, match=r"db0\.async_atomic"):
            obj.value = 2
        mutation_attempted.set()
        atomic_can_exit.set()

    await asyncio.wait_for(asyncio.gather(run_atomic(), run_mutation()), timeout=10)
    assert mutation_attempted.is_set()
    assert obj.value == 0
    obj.value = 2
    assert obj.value == 2


def test_commit_from_other_thread_waits_for_atomic_owner(db0_no_autocommit):
    obj = MemoTestClass(0)
    atomic_started = threading.Event()
    commit_attempting = threading.Event()
    atomic_can_exit = threading.Event()
    commit_done = threading.Event()
    errors = []

    def run_atomic():
        try:
            with db0.atomic():
                obj.value = 1
                atomic_started.set()
                assert commit_attempting.wait(timeout=5)
                assert not commit_done.wait(timeout=0.1)
                atomic_can_exit.set()
        except BaseException as exc:
            errors.append(exc)

    def run_commit():
        try:
            assert atomic_started.wait(timeout=5)
            commit_attempting.set()
            db0.commit()
            commit_done.set()
        except BaseException as exc:
            errors.append(exc)

    atomic_thread = threading.Thread(target=run_atomic)
    commit_thread = threading.Thread(target=run_commit)
    atomic_thread.start()
    commit_thread.start()
    atomic_thread.join(timeout=10)
    commit_thread.join(timeout=10)

    assert not atomic_thread.is_alive()
    assert not commit_thread.is_alive()
    assert atomic_can_exit.is_set()
    assert commit_done.is_set()
    assert errors == []
    assert obj.value == 1


def test_commit_inside_atomic_is_rejected(db0_no_autocommit):
    obj = MemoTestClass(0)

    with db0.atomic():
        obj.value = 1
        with pytest.raises(RuntimeError, match="db0\\.commit cannot run inside an active db0\\.atomic"):
            db0.commit()

    db0.commit()
    assert obj.value == 1


def test_atomic_cancel_type_change_then_close_does_not_corrupt_gc0(run_pytest_child):
    run_pytest_child(
        "python_tests/test_atomic.py::test_atomic_cancel_type_change_then_close_does_not_corrupt_gc0_child",
        env_flag="DB0_ATOMIC_TYPE_CHANGE_CLOSE_CHILD",
        failure_label="atomic cancel type-change child",
    )


def test_atomic_cancel_type_change_then_close_does_not_corrupt_gc0_child(db0_no_autocommit):
    obj = MemoTestClass(1)
    other = MemoTestClass(2)
    db0.commit()

    with db0.atomic() as atomic:
        obj.value = "outer"
        atomic.cancel()

    assert obj.value == 1
    other.value = "ok"
    db0.close()


def test_atomic_cancel_tuple_value_restores_wrapper_state(run_pytest_child):
    run_pytest_child(
        "python_tests/test_atomic.py::test_atomic_cancel_tuple_value_restores_wrapper_state_child",
        env_flag="DB0_ATOMIC_CANCEL_TUPLE_VALUE_CHILD",
        failure_label="atomic cancel tuple-value child",
    )


def test_atomic_cancel_tuple_value_restores_wrapper_state_child(db0_no_autocommit):
    obj = MemoTestClass(("initial",))
    db0.commit()

    with db0.atomic() as atomic:
        obj.value = ("atomic", 0)
        atomic.cancel()

    assert obj.value == ("initial",)

    with db0.atomic():
        obj.value = ("atomic", 1)
    db0.commit()

    assert obj.value == ("atomic", 1)


def test_atomic_cancel_tuple_value_releases_allocator_state(run_pytest_child):
    run_pytest_child(
        "python_tests/test_atomic.py::test_atomic_cancel_tuple_value_releases_allocator_state_child",
        env_flag="DB0_ATOMIC_CANCEL_TUPLE_ALLOCATOR_CHILD",
        failure_label="atomic cancel tuple allocator child",
    )


def test_atomic_cancel_tuple_value_releases_allocator_state_child(db0_no_autocommit):
    # A canceled tuple assignment must release only its own atomic allocation state.
    obj = MemoTestClass(0)
    db0.commit()

    with db0.atomic() as atomic:
        obj.value = ("atomic",)
        atomic.cancel()

    assert obj.value == 0
    db0.commit()


def test_atomic_cancel_string_value_restores_refcounted_member_state(run_pytest_child):
    run_pytest_child(
        "python_tests/test_atomic.py::test_atomic_cancel_string_value_restores_refcounted_member_state_child",
        env_flag="DB0_ATOMIC_CANCEL_STRING_VALUE_CHILD",
        failure_label="atomic cancel string-value child",
    )


@pytest.mark.skipif(
    os.environ.get("DB0_ATOMIC_CANCEL_STRING_VALUE_CHILD") != "1",
    reason="executed by test_atomic_cancel_string_value_restores_refcounted_member_state",
)
def test_atomic_cancel_string_value_restores_refcounted_member_state_child(db0_no_autocommit):
    obj = MemoTestClass("initial")
    db0.commit()

    with db0.atomic() as atomic:
        obj.value = "outer"
        atomic.cancel()

    # Regression for prefix-cache rollback: canceling a later allocation must
    # not expose an older cached lock and hide the committed string allocation.
    assert obj.value == "initial"
    db0.close()


def test_atomic_thread_constructor_waits_at_api_boundary_before_cancel(run_pytest_child):
    run_pytest_child(
        "python_tests/test_atomic.py::test_atomic_thread_constructor_waits_at_api_boundary_before_cancel_child",
        env_flag="DB0_ATOMIC_THREAD_CONSTRUCTOR_WAIT_CHILD",
        failure_label="atomic/thread constructor child",
    )


@pytest.mark.skipif(
    os.environ.get("DB0_ATOMIC_THREAD_CONSTRUCTOR_WAIT_CHILD") != "1",
    reason="executed by test_atomic_thread_constructor_waits_at_api_boundary_before_cancel",
)
def test_atomic_thread_constructor_waits_at_api_boundary_before_cancel_child(db0_no_autocommit):
    # A non-owner thread constructing a durable object must wait until the active atomic owner cancels and releases rollback state.
    obj = MemoTestClass(0)
    db0.commit()
    atomic_started = threading.Event()
    constructor_attempting = threading.Event()
    constructor_done = threading.Event()
    errors = []

    def run_constructor():
        try:
            assert atomic_started.wait(timeout=5)
            constructor_attempting.set()
            created = MemoTestClass(("thread-created",))
            assert created.value == ("thread-created",)
            constructor_done.set()
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_constructor)
    thread.start()

    with db0.atomic() as atomic:
        obj.value = 1
        atomic_started.set()
        assert constructor_attempting.wait(timeout=5)
        assert not constructor_done.wait(timeout=0.1)
        atomic.cancel()

    thread.join(timeout=5)
    assert not thread.is_alive()
    assert errors == []
    assert constructor_done.is_set()
    assert obj.value == 0
    db0.commit()


@pytest.mark.stress_test
def test_atomic_async_cancel_while_thread_constructs_objects_does_not_corrupt_state(run_pytest_child):
    # Timing-sensitive allocator/deferred-free repro. It may need multiple runs
    # to reproduce a failure or to build confidence that a fix is error-free.
    duration = float(os.environ.get("DB0_ATOMIC_ASYNC_THREAD_CONSTRUCT_SECONDS", "5"))
    run_pytest_child(
        "python_tests/test_atomic.py::test_atomic_async_cancel_while_thread_constructs_objects_does_not_corrupt_state_child",
        env_flag="DB0_ATOMIC_ASYNC_THREAD_CONSTRUCT_CHILD",
        timeout=duration + 5,
        failure_label="atomic async/thread construction child",
    )


@pytest.mark.skipif(
    os.environ.get("DB0_ATOMIC_ASYNC_THREAD_CONSTRUCT_CHILD") != "1",
    reason="executed by test_atomic_async_cancel_while_thread_constructs_objects_does_not_corrupt_state",
)
async def test_atomic_async_cancel_while_thread_constructs_objects_does_not_corrupt_state_child(db0_no_autocommit):
    duration = float(os.environ.get("DB0_ATOMIC_ASYNC_THREAD_CONSTRUCT_SECONDS", "5"))
    objects = [MemoTestClass(i) for i in range(4)]
    log = db0.list()
    index = db0.index()
    for key, obj in enumerate(objects):
        index.add(key, obj)
        log.append(obj)
    db0.commit()
    errors = []
    stop = threading.Event()
    deadline = time.monotonic() + duration
    async_atomic_gate = asyncio.Lock()

    async def async_atomic_owner(task_id):
        rng = random.Random(0xA70B000 + task_id)
        iteration = 0
        while time.monotonic() < deadline and not stop.is_set():
            iteration += 1
            obj = objects[(iteration + task_id) % len(objects)]
            async with async_atomic_gate:
                async with db0.async_atomic() as outer:
                    obj.value = ("async-outer", task_id, iteration)
                    await asyncio.sleep(rng.random() / 1000)
                    async with db0.async_atomic() as inner:
                        item = MemoTestClass(("async-log", task_id, iteration))
                        log.append(item)
                        index.add(10_000_000 + task_id * 1_000_000 + iteration, obj)
                        if rng.random() < 0.5:
                            inner.cancel()
                    if rng.random() < 0.25:
                        outer.cancel()
            await asyncio.sleep(0)

    def thread_constructor(worker_id):
        rng = random.Random(0xA70C000 + worker_id)
        iteration = 0
        try:
            while time.monotonic() < deadline and not stop.is_set():
                obj = objects[rng.randrange(len(objects))]
                mode = rng.randrange(4)
                iteration += 1

                if mode == 0:
                    obj.value = ("thread-plain", worker_id, iteration)
                elif mode == 1:
                    with db0.atomic() as atomic:
                        obj.value = ("thread-atomic", worker_id, iteration)
                        log.append(MemoTestClass(("thread-log", worker_id, iteration)))
                        if rng.random() < 0.35:
                            atomic.cancel()
                elif mode == 2:
                    with db0.atomic() as outer:
                        obj.value = ("thread-outer", worker_id, iteration)
                        with db0.atomic() as inner:
                            nested = objects[(rng.randrange(len(objects)) + worker_id) % len(objects)]
                            nested.value = ("thread-inner", worker_id, iteration)
                            index.add(worker_id * 1_000_000 + iteration, nested)
                            if rng.random() < 0.35:
                                inner.cancel()
                        if rng.random() < 0.35:
                            outer.cancel()
                elif rng.random() < 0.5:
                    db0.commit()
                else:
                    _ = obj.value
        except BaseException as exc:
            errors.append(exc)
            stop.set()

    threads = [threading.Thread(target=thread_constructor, args=(0,))]
    for thread in threads:
        thread.start()
    try:
        await asyncio.wait_for(async_atomic_owner(0), timeout=duration + 2)
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    
    
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
    # This is a focused repro for the pre-existing Index null-key lifecycle path.
    # It creates an index in atomic and stores an object under None.
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
    # This isolates multi-prefix atomic lifecycle handling separately from
    # async_atomic ownership checks.
    prefix = "test-data"
    obj = MemoScopedClass(None, prefix=prefix)    
    with db0.atomic():
        obj.value = db0.index()
        obj.value.add(None, MemoScopedClass(100, prefix=prefix))
    
    assert len(list(obj.value.select(None, 100, null_first=True))) == 1
    

def test_multiple_atomic_index_updates_with_multiple_prefixes_issue_1(db0_fixture):
    # Same cross-prefix index lifecycle family as test_atomic_with_multiple_prefixes.
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
    # Same cross-prefix index lifecycle family as test_atomic_with_multiple_prefixes.
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


@pytest.mark.stress_test
@pytest.mark.skip(reason=ATOMIC_INDEX_ITERATOR_REPRO_SKIP)
def test_atomic_index_iterator_survives_canceled_atomic_context_stress(run_pytest_child):
    # Timing-sensitive iterator lifetime repro. It may need multiple runs to
    # reproduce a failure or to build confidence that a fix is error-free.
    duration = float(os.environ.get("DB0_ATOMIC_INDEX_ITERATOR_SECONDS", "10"))
    run_pytest_child(
        "python_tests/test_atomic.py::test_atomic_index_iterator_survives_canceled_atomic_context_stress_child",
        env_flag="DB0_ATOMIC_INDEX_ITERATOR_CHILD",
        timeout=duration + 10,
        failure_label="atomic index iterator/canceled atomic stress child",
        pytest_args=("-o", "faulthandler_timeout=10"),
    )


@pytest.mark.skipif(
    os.environ.get("DB0_ATOMIC_INDEX_ITERATOR_CHILD") != "1",
    reason="stress workload is executed by test_atomic_index_iterator_survives_canceled_atomic_context_stress",
)
def test_atomic_index_iterator_survives_canceled_atomic_context_stress_child(db0_no_autocommit):
    duration = float(os.environ.get("DB0_ATOMIC_INDEX_ITERATOR_SECONDS", "10"))
    deadline = time.monotonic() + duration
    stop_threads = threading.Event()
    iterator_ready = threading.Event()
    rollback_done = threading.Event()
    errors = []
    counters_lock = threading.Lock()
    counters = {
        "iterators": 0,
        "rollbacks": 0,
        "commits": 0,
    }

    objects = [MemoTestClass(("seed", index)) for index in range(32)]
    index = db0.index()
    for key, obj in enumerate(objects):
        index.add(key, obj)
    db0.commit()

    def inc(name, value=1):
        with counters_lock:
            counters[name] += value

    def remember_error(exc):
        with counters_lock:
            errors.append(exc)

    def iterator_worker():
        rng = random.Random(0x170A70C)
        try:
            while not stop_threads.is_set() and time.monotonic() < deadline:
                rollback_done.clear()
                iterator = iter(index.select(0, 100_000_000))
                for _ in range(rng.randrange(1, 4)):
                    if next(iterator, None) is None:
                        break

                iterator_ready.set()
                rollback_done.wait(timeout=1.0)

                for _ in range(16):
                    try:
                        next(iterator)
                    except StopIteration:
                        break
                inc("iterators")
        except BaseException as exc:
            remember_error(exc)
            stop_threads.set()
            rollback_done.set()

    def rollback_worker():
        rng = random.Random(0x170A70D)
        iteration = 0
        try:
            while not stop_threads.is_set() and time.monotonic() < deadline:
                if not iterator_ready.wait(timeout=1.0):
                    continue
                iterator_ready.clear()
                iteration += 1

                with db0.atomic() as atomic:
                    for offset in range(8):
                        obj = objects[(iteration + offset) % len(objects)]
                        obj.value = ("rolled-back", iteration, offset)
                        index.add(1_000_000 + iteration * 16 + offset, obj)
                    atomic.cancel()
                inc("rollbacks")

                if rng.random() < 0.25:
                    db0.commit()
                    inc("commits")
                rollback_done.set()
        except BaseException as exc:
            remember_error(exc)
            stop_threads.set()
            rollback_done.set()

    threads = [
        threading.Thread(target=iterator_worker),
        threading.Thread(target=rollback_worker),
    ]
    for thread in threads:
        thread.start()

    try:
        while time.monotonic() < deadline and not stop_threads.is_set():
            time.sleep(0.001)
    finally:
        stop_threads.set()
        iterator_ready.set()
        rollback_done.set()
        for thread in threads:
            thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    if errors:
        pytest.fail(repr(errors[0]))
    assert counters["iterators"] > 0
    assert counters["rollbacks"] > 0


@pytest.mark.stress_test
@pytest.mark.skip(reason=ATOMIC_STRESS_REPRO_SKIP)
def test_atomic_async_thread_deadlock_detection_stress(run_pytest_child):
    duration = float(os.environ.get("DB0_ATOMIC_STRESS_SECONDS", "60"))
    run_pytest_child(
        "python_tests/test_atomic.py::test_atomic_async_thread_deadlock_detection_stress_child",
        env_flag="DB0_ATOMIC_STRESS_CHILD",
        timeout=duration + 30,
        failure_label="atomic async/thread stress child",
        pytest_args=("-o", "faulthandler_timeout=10"),
    )


@pytest.mark.skipif(
    os.environ.get("DB0_ATOMIC_STRESS_CHILD") != "1",
    reason="stress workload is executed by test_atomic_async_thread_deadlock_detection_stress",
)
async def test_atomic_async_thread_deadlock_detection_stress_child(db0_no_autocommit):
    duration = float(os.environ.get("DB0_ATOMIC_STRESS_SECONDS", "60"))
    deadline = time.monotonic() + duration
    stop_threads = threading.Event()
    errors = []
    async_atomic_gate = asyncio.Lock()
    counters_lock = threading.Lock()
    counters = {
        "thread_ops": 0,
        "async_ops": 0,
        "deadlocks": 0,
        "thread_cancels": 0,
        "async_cancels": 0,
    }

    root = MemoTestClass({"thread": 0, "async": 0, "last": None})
    objects = [MemoTestClass(i) for i in range(16)]
    log = db0.list()
    index = db0.index()
    for key, obj in enumerate(objects):
        index.add(key, obj)
        log.append(obj)
    db0.commit()

    def inc(name, value=1):
        with counters_lock:
            counters[name] += value

    def remember_error(exc):
        with counters_lock:
            errors.append(exc)

    def maybe_cancel(atomic, rng, counter_name):
        if rng.random() < 0.35:
            atomic.cancel()
            inc(counter_name)
            return True
        return False

    def thread_worker(worker_id):
        rng = random.Random(0xA70C000 + worker_id)
        iteration = 0
        try:
            while not stop_threads.is_set() and time.monotonic() < deadline:
                obj = objects[rng.randrange(len(objects))]
                mode = rng.randrange(7)
                iteration += 1
                step = "unknown"

                if mode == 0:
                    step = "thread_plain_set"
                    obj.value = ("thread-plain", worker_id, iteration)
                elif mode in (1, 2):
                    step = "thread_atomic_set_root_log"
                    with db0.atomic() as atomic:
                        step = "thread_atomic_obj_set"
                        obj.value = ("thread-atomic", worker_id, iteration)
                        step = "thread_atomic_root_increment"
                        root.value["thread"] = root.value["thread"] + 1
                        step = "thread_atomic_log_append"
                        log.append(MemoTestClass(("thread-log", worker_id, iteration)))
                        step = "thread_atomic_maybe_cancel"
                        maybe_cancel(atomic, rng, "thread_cancels")
                elif mode in (3, 4):
                    step = "thread_nested_atomic"
                    with db0.atomic() as outer:
                        step = "thread_nested_outer_set"
                        obj.value = ("thread-outer", worker_id, iteration)
                        with db0.atomic() as inner:
                            step = "thread_nested_pick"
                            nested = objects[(rng.randrange(len(objects)) + worker_id) % len(objects)]
                            step = "thread_nested_inner_set"
                            nested.value = ("thread-inner", worker_id, iteration)
                            step = "thread_nested_index_add"
                            index.add(worker_id * 1_000_000 + iteration, nested)
                            step = "thread_nested_inner_maybe_cancel"
                            maybe_cancel(inner, rng, "thread_cancels")
                        step = "thread_nested_outer_maybe_cancel"
                        maybe_cancel(outer, rng, "thread_cancels")
                elif mode == 5:
                    step = "thread_atomic_tags"
                    with db0.atomic():
                        tag = f"atomic-thread-{worker_id}-{iteration % 11}"
                        step = "thread_atomic_tags_add"
                        db0.tags(obj).add(tag)
                        step = "thread_atomic_tags_root_last"
                        root.value["last"] = tag
                elif mode == 6:
                    if rng.random() < 0.5:
                        step = "thread_commit"
                        db0.commit()
                    else:
                        step = "thread_atomic_root_only"
                        with db0.atomic() as atomic:
                            step = "thread_atomic_root_only_increment"
                            root.value["thread"] = root.value["thread"] + 1
                            step = "thread_atomic_root_only_maybe_cancel"
                            maybe_cancel(atomic, rng, "thread_cancels")
                else:
                    step = "thread_read_value"
                    _ = obj.value

                inc("thread_ops")
        except BaseException as exc:
            remember_error((step, exc))
            stop_threads.set()

    async def async_deadlock_probe(task_id):
        rng = random.Random(0xA70A000 + task_id)
        probe_index = 0
        while time.monotonic() < deadline and not stop_threads.is_set():
            owner_started = asyncio.Event()
            mutation_attempted = asyncio.Event()
            obj = objects[(probe_index + task_id) % len(objects)]
            probe_index += 1

            async def owner():
                async with async_atomic_gate:
                    async with db0.async_atomic() as atomic:
                        obj.value = ("async-owner", task_id, probe_index)
                        owner_started.set()
                        await asyncio.wait_for(mutation_attempted.wait(), timeout=2.0)
                        root.value["async"] = root.value["async"] + 1
                        if rng.random() < 0.4:
                            atomic.cancel()
                            inc("async_cancels")

            async def same_thread_mutator():
                await asyncio.wait_for(owner_started.wait(), timeout=2.0)
                with pytest.raises(RuntimeError, match=r"db0\.async_atomic"):
                    obj.value = ("async-forbidden", task_id, probe_index)
                inc("deadlocks")
                mutation_attempted.set()

            await asyncio.gather(owner(), same_thread_mutator())
            inc("async_ops")
            await asyncio.sleep(0)

    async def async_nested_worker(task_id):
        rng = random.Random(0xA70B000 + task_id)
        iteration = 0
        while time.monotonic() < deadline and not stop_threads.is_set():
            iteration += 1
            obj = objects[(iteration + task_id) % len(objects)]
            async with async_atomic_gate:
                async with db0.async_atomic() as outer:
                    obj.value = ("async-outer", task_id, iteration)
                    await asyncio.sleep(rng.random() / 1000)
                    async with db0.async_atomic() as inner:
                        log.append(MemoTestClass(("async-log", task_id, iteration)))
                        index.add(10_000_000 + task_id * 1_000_000 + iteration, obj)
                        if rng.random() < 0.5:
                            inner.cancel()
                            inc("async_cancels")
                    if rng.random() < 0.25:
                        outer.cancel()
                        inc("async_cancels")
            inc("async_ops")
            await asyncio.sleep(0)

    threads = [threading.Thread(target=thread_worker, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()

    try:
        await asyncio.wait_for(asyncio.gather(
            async_deadlock_probe(0),
            async_deadlock_probe(1),
            async_nested_worker(0),
            async_nested_worker(1),
        ), timeout=duration + 10)
    finally:
        stop_threads.set()
        for thread in threads:
            thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    if errors:
        pytest.fail(repr(errors[0]))
    assert counters["deadlocks"] > 0
    assert counters["thread_ops"] > 0
    assert counters["async_ops"] > 0


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
