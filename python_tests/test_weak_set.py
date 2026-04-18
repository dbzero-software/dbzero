# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestPxClass


def test_weak_set_created_empty(db0_fixture):
    ws = db0.weak_set()
    assert ws is not None
    assert len(ws) == 0


def test_weak_set_from_iterable(db0_fixture):
    obj_1 = MemoTestClass(1)
    obj_2 = MemoTestClass(2)
    ws = db0.weak_set([obj_1, obj_2])
    assert len(ws) == 2


def test_weak_set_primitives_rejected(db0_fixture):
    with pytest.raises((TypeError, Exception)):
        db0.weak_set([1])


def test_weak_set_contains_by_actual_instance_same_prefix(db0_fixture):
    obj_1 = MemoTestClass(1)
    obj_2 = MemoTestClass(2)
    ws = db0.weak_set([obj_1])
    assert obj_1 in ws
    assert obj_2 not in ws


def test_weak_set_contains_by_actual_instance_cross_prefix(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    ws = MemoTestPxClass(db0.weak_set([obj_1]), prefix=px_2)
    assert obj_1 in ws.value


def test_weak_set_iter_dereferences(db0_fixture):
    obj_1 = MemoTestClass(1)
    obj_2 = MemoTestClass(2)
    ws = db0.weak_set([obj_1, obj_2])
    seen = list(ws)
    assert len(seen) == 2
    assert obj_1 in seen
    assert obj_2 in seen


def test_weak_set_len_after_del(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    ws = MemoTestPxClass(db0.weak_set([obj_1]), prefix=px_2)
    assert len(ws.value) == 1
    del obj_1
    db0.commit()
    assert len(ws.value) == 0


def test_weak_set_iter_skips_expired(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(1, prefix=px_1)
    obj_2 = MemoTestPxClass(2, prefix=px_1)
    ws = MemoTestPxClass(db0.weak_set([obj_1, obj_2]), prefix=px_2)
    del obj_1
    db0.commit()
    seen = list(ws.value)
    assert len(seen) == 1
    assert seen[0] == obj_2


def test_weak_set_add_discard_remove(db0_fixture):
    obj_1 = MemoTestClass(1)
    obj_2 = MemoTestClass(2)
    ws = db0.weak_set()
    ws.add(obj_1)
    ws.add(obj_2)
    assert len(ws) == 2
    ws.discard(obj_1)
    assert len(ws) == 1
    assert obj_1 not in ws
    ws.remove(obj_2)
    assert len(ws) == 0
    with pytest.raises(KeyError):
        ws.remove(obj_1)


def test_weak_set_does_not_increment_refcount(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    cnt_before = db0.getrefcount(obj_1)
    ws = MemoTestPxClass(db0.weak_set([obj_1]), prefix=px_2)
    assert db0.getrefcount(obj_1) == cnt_before


def test_weak_set_copy(db0_fixture):
    obj_1 = MemoTestClass(1)
    obj_2 = MemoTestClass(2)
    ws = db0.weak_set([obj_1, obj_2])
    ws_copy = ws.copy()
    assert len(ws_copy) == 2
    assert obj_1 in ws_copy
    assert obj_2 in ws_copy


def test_weak_set_persistence(db0_fixture):
    obj_1 = MemoTestClass(1)
    holder = MemoTestClass(db0.weak_set([obj_1]))
    db0.commit()
    assert len(holder.value) == 1
    assert obj_1 in holder.value
