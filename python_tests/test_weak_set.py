# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestPxClass, MemoScopedSingleton
from .conftest import DB0_DIR


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


def test_weak_set_persistence_across_close(db0_fixture):
    px_current = db0.get_current_prefix().name
    px_other = "some-other-prefix"
    db0.open(px_other, "rw")

    n = 120
    objs = [
        MemoTestPxClass(i, prefix=px_current if i % 2 == 0 else px_other)
        for i in range(n)
    ]

    holder = MemoTestPxClass(db0.weak_set(objs), prefix=px_other)
    assert len(holder.value) == n

    # Per-prefix singletons anchor the objects and the holder so they
    # survive close/reopen and the weak refs in the set stay resolvable.
    MemoScopedSingleton(
        value=[o for o in objs if o.value % 2 == 0], prefix=px_current
    )
    MemoScopedSingleton(
        value=[holder] + [o for o in objs if o.value % 2 == 1],
        prefix=px_other,
    )

    obj_values = sorted(o.value for o in objs)

    del holder, objs
    db0.commit()
    db0.close()

    db0.init(DB0_DIR)
    db0.open(px_current, "r")
    db0.open(px_other, "r")

    root_other = db0.fetch(MemoScopedSingleton, prefix=px_other)
    holder2 = root_other.value[0]
    ws = holder2.value
    assert len(ws) == n

    root_current = db0.fetch(MemoScopedSingleton, prefix=px_current)
    all_objs = list(root_current.value) + list(root_other.value[1:])
    assert sorted(o.value for o in all_objs) == obj_values
    for o in all_objs:
        assert o in ws


def test_weak_set_persistence_only_holder_prefix_opened(db0_fixture):
    # After close, reopen ONLY the prefix that owns the weak_set.
    # Half the members are long weak refs into a prefix that is not loaded.
    # Documents the current behavior: every set-wide operation on such a
    # weak_set raises because the foreign prefix cannot be resolved.
    px_current = db0.get_current_prefix().name
    px_other = "some-other-prefix"
    db0.open(px_other, "rw")

    n = 120
    objs = [
        MemoTestPxClass(i, prefix=px_current if i % 2 == 0 else px_other)
        for i in range(n)
    ]

    holder = MemoTestPxClass(db0.weak_set(objs), prefix=px_other)
    assert len(holder.value) == n

    MemoScopedSingleton(
        value=[o for o in objs if o.value % 2 == 0], prefix=px_current
    )
    MemoScopedSingleton(
        value=[holder] + [o for o in objs if o.value % 2 == 1],
        prefix=px_other,
    )

    del holder, objs
    db0.commit()
    db0.close()

    db0.init(DB0_DIR)
    # Deliberately open only the holder's prefix — the other one stays closed.
    db0.open(px_other, "r")

    root_other = db0.fetch(MemoScopedSingleton, prefix=px_other)
    holder2 = root_other.value[0]
    ws = holder2.value
    same_prefix_objs = list(root_other.value[1:])
    # Sanity: same-prefix anchors are fully reachable on their own.
    assert len(same_prefix_objs) == n // 2

    # len(), iteration, and membership all traverse the full underlying
    # storage and hit the unresolvable long weak refs into px_current.
    with pytest.raises(Exception):
        len(ws)
    with pytest.raises(RuntimeError, match="Prefix:"):
        list(ws)
    with pytest.raises(RuntimeError, match="Prefix:"):
        _ = same_prefix_objs[0] in ws

    # Opening the missing prefix heals the set.
    db0.open(px_current, "r")
    assert len(ws) == n
    for o in same_prefix_objs:
        assert o in ws
