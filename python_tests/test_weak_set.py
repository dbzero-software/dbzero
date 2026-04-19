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


def test_weak_set_discard_mixed_prefix(db0_fixture):
    # Minimal reproducer: a weak_set that holds exactly one object
    # from the current prefix (short weak ref) and one from another
    # prefix (long weak ref). discard() on either element is expected
    # to remove it; today it silently does nothing and len stays at 2.
    px_current = db0.get_current_prefix().name
    px_other = "some-other-prefix"
    db0.open(px_other, "rw")

    obj_curr = MemoTestPxClass(1, prefix=px_current)
    obj_other = MemoTestPxClass(2, prefix=px_other)

    ws = db0.weak_set()
    ws.add(obj_curr)
    ws.add(obj_other)
    assert len(ws) == 2
    assert obj_curr in ws
    assert obj_other in ws

    # Discarding the current-prefix (short weak ref) member: the
    # long weak ref from the other prefix must remain.
    ws.discard(obj_curr)
    assert obj_curr not in ws
    assert obj_other in ws
    assert len(ws) == 1

    # And discarding the remaining cross-prefix member empties the set.
    ws.discard(obj_other)
    assert obj_other not in ws
    assert len(ws) == 0


def test_weak_set_bulk_add_discard_contains(db0_fixture):
    import random

    # Mix objects from two prefixes so the weak_set holds both
    # same-prefix weak refs and cross-prefix "long" weak refs.
    px_current = db0.get_current_prefix().name
    px_other = "some-other-prefix"
    db0.open(px_other, "rw")

    n = 128
    objs = [
        MemoTestPxClass(i, prefix=px_current if i % 2 == 0 else px_other)
        for i in range(n)
    ]

    # Populate ws and verify every element is a member.
    ws = db0.weak_set()
    for o in objs:
        ws.add(o)
    assert len(ws) == n
    for o in objs:
        assert o in ws

    # Objects not added — from either prefix — must not be reported as members.
    assert MemoTestPxClass(9998, prefix=px_current) not in ws
    assert MemoTestPxClass(9999, prefix=px_other) not in ws

    # Discard a random half, then confirm membership on both sides.
    # Split by id() so memo value-equality cannot move objects between groups.
    rng = random.Random(42)
    to_discard = rng.sample(objs, n // 2)
    discard_ids = {id(o) for o in to_discard}
    remaining = [o for o in objs if id(o) not in discard_ids]

    for o in to_discard:
        ws.discard(o)

    assert len(ws) == n - len(to_discard)
    for o in to_discard:
        assert o not in ws
    for o in remaining:
        assert o in ws
