# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import asyncio
import importlib
import threading

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass


def assert_read_only_mutation_rejected(callback):
    with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
        with db0.read_only():
            callback()


def test_read_only_allows_reads(db0_fixture):
    obj = MemoTestClass(123)

    with db0.read_only():
        assert obj.value == 123
        assert db0.fetch(db0.uuid(obj)) == obj


def test_read_only_rejects_memo_field_assignment(db0_fixture):
    obj = MemoTestClass(123)

    assert_read_only_mutation_rejected(lambda: setattr(obj, "value", 456))
    assert obj.value == 123


def test_read_only_rejects_container_mutations(db0_fixture):
    list_obj = MemoTestClass([1])
    set_obj = MemoTestClass(set([1]))
    dict_obj = MemoTestClass({"a": 1})
    bytearray_obj = db0.bytearray(b"abc")

    assert_read_only_mutation_rejected(lambda: list_obj.value.append(2))
    assert_read_only_mutation_rejected(lambda: set_obj.value.add(2))
    assert_read_only_mutation_rejected(lambda: dict_obj.value.__setitem__("b", 2))
    assert_read_only_mutation_rejected(lambda: bytearray_obj.__setitem__(0, ord("z")))

    assert list_obj.value == [1]
    assert set_obj.value == set([1])
    assert dict_obj.value == {"a": 1}
    assert len(bytearray_obj) == 3
    assert bytearray_obj[0] == ord("a")


def test_read_only_rejects_tags_touch_and_index_mutations(db0_fixture):
    obj = MemoTestClass(123)
    index = db0.index()

    assert_read_only_mutation_rejected(lambda: db0.tags(obj).add("tag1"))
    assert_read_only_mutation_rejected(lambda: db0.touch(obj))
    assert_read_only_mutation_rejected(lambda: index.add(1, obj))

    assert len(list(db0.find("tag1"))) == 0
    assert len(index) == 0


def test_read_only_rejects_new_durable_object_creation(db0_fixture):
    assert_read_only_mutation_rejected(lambda: MemoTestClass(123))


def test_read_only_rejects_memo_init_before_python_init(db0_fixture):
    init_calls = []

    @db0.memo
    class ReadOnlyInitProbe:
        def __init__(self):
            init_calls.append("called")
            self.value = 123

    assert_read_only_mutation_rejected(lambda: ReadOnlyInitProbe())
    assert init_calls == []


def test_nested_read_only_contexts_restore_depth(db0_fixture):
    obj = MemoTestClass(123)

    with db0.read_only():
        with db0.read_only():
            with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
                obj.value = 456

        with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
            obj.value = 789

    obj.value = 951
    assert obj.value == 951


def test_read_only_restores_depth_after_exception(db0_fixture):
    obj = MemoTestClass(123)

    with pytest.raises(ValueError, match="expected"):
        with db0.read_only():
            raise ValueError("expected")

    obj.value = 456
    assert obj.value == 456


def test_read_only_global_depth_returns_to_zero(db0_fixture):
    obj = MemoTestClass(123)

    for _ in range(1000):
        with db0.read_only():
            assert obj.value == 123

    obj.value = 456
    assert obj.value == 456


def test_read_only_inside_atomic_rejects_mutation(db0_fixture):
    obj = MemoTestClass(123)

    with db0.atomic():
        with db0.read_only():
            with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
                obj.value = 456

    assert obj.value == 123


def test_atomic_inside_read_only_is_optimized_out(db0_fixture, monkeypatch):
    obj = MemoTestClass(123)
    atomic_module = importlib.import_module("dbzero.atomic")

    with db0.read_only():
        assert db0._in_read_only()
        monkeypatch.setattr(
            atomic_module,
            "begin_atomic",
            lambda: pytest.fail("begin_atomic should not run inside read_only"),
        )
        with db0.atomic():
            with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
                obj.value = 456

    assert not db0._in_read_only()
    obj.value = 789
    assert obj.value == 789


def test_read_only_context_is_thread_local(db0_fixture):
    obj = MemoTestClass(123)
    iterations = 1000
    start = threading.Event()
    errors = []

    def run_read_only():
        try:
            assert start.wait(timeout=5)
            for _ in range(iterations):
                with db0.read_only():
                    assert obj.value >= 123
                    with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
                        obj.value = 456
                    assert obj.value >= 123
        except BaseException as exc:
            errors.append(exc)

    def run_nested_read_only():
        try:
            assert start.wait(timeout=5)
            for _ in range(iterations):
                with db0.read_only():
                    with db0.read_only():
                        assert obj.value >= 123
                        with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
                            obj.value = 654
                        assert obj.value >= 123
        except BaseException as exc:
            errors.append(exc)

    def run_mutation():
        try:
            assert start.wait(timeout=5)
            for value in range(789, 789 + iterations):
                obj.value = value
                assert obj.value >= 789
        except BaseException as exc:
            errors.append(exc)

    read_only_thread = threading.Thread(target=run_read_only)
    nested_read_only_thread = threading.Thread(target=run_nested_read_only)
    mutation_thread = threading.Thread(target=run_mutation)
    read_only_thread.start()
    nested_read_only_thread.start()
    mutation_thread.start()
    start.set()
    read_only_thread.join(timeout=10)
    nested_read_only_thread.join(timeout=10)
    mutation_thread.join(timeout=10)

    assert not read_only_thread.is_alive()
    assert not nested_read_only_thread.is_alive()
    assert not mutation_thread.is_alive()
    assert errors == []
    assert obj.value >= 789


def test_read_only_long_lived_context_is_thread_local(db0_fixture):
    obj = MemoTestClass(123)
    iterations = 1000
    read_only_started = threading.Event()
    stop_mutating = threading.Event()
    errors = []

    def run_read_only():
        try:
            with db0.read_only():
                read_only_started.set()
                for _ in range(iterations):
                    assert obj.value >= 123
                    with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
                        obj.value = 456
                    assert obj.value >= 123
        except BaseException as exc:
            errors.append(exc)
        finally:
            stop_mutating.set()

    def run_mutation():
        try:
            assert read_only_started.wait(timeout=5)
            value = 789
            while not stop_mutating.is_set():
                obj.value = value
                value += 1
            obj.value = value
        except BaseException as exc:
            errors.append(exc)

    read_only_thread = threading.Thread(target=run_read_only)
    mutation_thread = threading.Thread(target=run_mutation)
    read_only_thread.start()
    mutation_thread.start()
    read_only_thread.join(timeout=10)
    mutation_thread.join(timeout=10)

    assert not read_only_thread.is_alive()
    assert not mutation_thread.is_alive()
    assert errors == []
    assert obj.value >= 789


async def test_read_only_context_should_not_leak_between_async_tasks(db0_fixture):
    obj = MemoTestClass(123)
    read_only_started = asyncio.Event()

    async def run_read_only():
        with db0.read_only():
            read_only_started.set()
            await asyncio.sleep(0.1)

    async def run_mutation():
        await read_only_started.wait()
        obj.value = 789

    await asyncio.wait_for(
        asyncio.gather(run_read_only(), run_mutation()),
        timeout=5,
    )
    assert obj.value == 789


async def test_read_only_context_applies_to_child_async_task_while_parent_is_active(db0_fixture):
    obj = MemoTestClass(123)

    async def mutate_in_child_task():
        with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
            obj.value = 789

    with db0.read_only():
        child_task = asyncio.create_task(mutate_in_child_task())
        await asyncio.wait_for(child_task, timeout=5)

    obj.value = 789
    assert obj.value == 789


async def test_read_only_context_does_not_outlive_parent_async_block(db0_fixture):
    obj = MemoTestClass(123)
    child_can_run = asyncio.Event()

    async def mutate_in_child_task():
        await child_can_run.wait()
        obj.value = 789

    with db0.read_only():
        child_task = asyncio.create_task(mutate_in_child_task())

    child_can_run.set()
    await asyncio.wait_for(child_task, timeout=5)

    assert obj.value == 789


def test_read_only_fast_overhead_paths(db0_fixture):
    obj = MemoTestClass(0)
    iterations = 100

    for _ in range(iterations):
        assert obj.value >= 0

    for value in range(iterations):
        obj.value = value

    with db0.read_only():
        for _ in range(iterations):
            assert obj.value >= 0

    with db0.read_only():
        for _ in range(iterations):
            with pytest.raises(RuntimeError, match="read_only.*mutation|mutation.*read_only"):
                obj.value = 1

    for _ in range(iterations):
        with db0.read_only():
            pass


async def test_read_only_fast_async_switch_paths(db0_fixture):
    obj = MemoTestClass(123)
    iterations = 25

    async def no_op():
        await asyncio.sleep(0)

    for _ in range(iterations):
        await no_op()
        assert obj.value == 123

    with db0.read_only():
        for _ in range(iterations):
            await no_op()
            assert obj.value == 123
