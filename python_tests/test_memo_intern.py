# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from dataclasses import dataclass

import pytest
import dbzero as db0


def get_memo_class_object(obj):
    return db0.get_memo_class(obj).get_class()


@db0.memo(immutable=True, intern=True)
@dataclass
class MemoInternLeaf:
    name: str


@db0.memo(immutable=True)
@dataclass
class MemoNonInternImmutableLeaf:
    name: str


@db0.memo
@dataclass
class MemoNonInternMutableLeaf:
    name: str


@db0.memo(immutable=True, intern=True)
class MemoInternHolder:
    def __init__(self, value):
        self.value = value


@db0.memo(immutable=True, intern=True)
class MemoInternContainerHolder:
    def __init__(self, values):
        self.values = values


def test_intern_flag_is_persisted_on_class(db0_fixture):
    obj = db0.materialized(MemoInternLeaf("alpha"))

    flags = get_memo_class_object(obj).get_type_flags()

    assert flags["immutable"] is True
    assert flags["intern"] is True


def test_intern_requires_immutable_decorator_flag():
    with pytest.raises(RuntimeError, match="intern.*immutable"):

        @db0.memo(intern=True)
        class MemoInvalidIntern:
            pass


def test_intern_flag_cannot_change_after_class_materialization(db0_fixture):
    @db0.memo(id="dbzero-software/dbzero/tests/intern-stable-contract", immutable=True, intern=True)
    class MemoInitiallyIntern:
        def __init__(self, name):
            self.name = name

    db0.materialized(MemoInitiallyIntern("alpha"))

    with pytest.raises(RuntimeError, match="intern flag"):

        @db0.memo(id="dbzero-software/dbzero/tests/intern-stable-contract", immutable=True)
        class MemoNoLongerIntern:
            def __init__(self, name):
                self.name = name

        db0.materialized(MemoNoLongerIntern("beta"))


def test_interned_object_can_reference_interned_immutable_instance(db0_fixture):
    leaf = MemoInternLeaf("nested")

    holder = db0.materialized(MemoInternHolder(leaf))

    assert holder.value.name == "nested"


def test_embedded_interned_object_reuses_embedded_instance(db0_fixture):
    leaf = MemoInternLeaf("embedded")
    holder = db0.materialized(MemoInternHolder(leaf))
    second = db0.materialized(MemoInternLeaf("embedded"))

    assert db0.uuid(leaf) == db0.uuid(holder.value)
    assert db0.uuid(second) == db0.uuid(leaf)
    assert leaf.name == "embedded"
    assert second.name == "embedded"


def test_embedded_interned_object_reuses_after_commit_and_fetch(db0_fixture):
    leaf = MemoInternLeaf("embedded committed")
    holder = db0.materialized(MemoInternHolder(leaf))
    db0.tags(holder).add("keep-embedded-intern")
    leaf_uuid = db0.uuid(leaf)
    holder_uuid = db0.uuid(holder)
    db0.commit()

    fetched_holder = db0.fetch(holder_uuid, MemoInternHolder)
    second = db0.materialized(MemoInternLeaf("embedded committed"))

    assert db0.uuid(fetched_holder.value) == leaf_uuid
    assert db0.uuid(second) == leaf_uuid
    assert second.name == "embedded committed"


def test_standalone_interned_object_reuses_existing_instance(db0_fixture):
    first = db0.materialized(MemoInternLeaf("dedupe"))
    second = db0.materialized(MemoInternLeaf("dedupe"))

    assert db0.uuid(second) == db0.uuid(first)
    assert second.name == "dedupe"


def test_standalone_interned_object_keeps_distinct_content(db0_fixture):
    first = db0.materialized(MemoInternLeaf("alpha"))
    second = db0.materialized(MemoInternLeaf("beta"))

    assert db0.uuid(second) != db0.uuid(first)
    assert first.name == "alpha"
    assert second.name == "beta"


def test_standalone_interned_object_reuses_after_commit_and_fetch(db0_fixture):
    first = db0.materialized(MemoInternLeaf("committed"))
    first_uuid = db0.uuid(first)
    db0.commit()

    fetched = db0.fetch(first_uuid, MemoInternLeaf)
    second = db0.materialized(MemoInternLeaf("committed"))

    assert db0.uuid(fetched) == first_uuid
    assert db0.uuid(second) == first_uuid
    assert second.name == "committed"


def test_interned_object_rejects_non_intern_immutable_reference(db0_fixture):
    with pytest.raises((RuntimeError, AttributeError), match="intern.*reference"):
        db0.materialized(MemoInternHolder(MemoNonInternImmutableLeaf("nested")))


def test_interned_object_rejects_mutable_reference(db0_fixture):
    with pytest.raises((RuntimeError, AttributeError), match="intern.*reference"):
        db0.materialized(MemoInternHolder(MemoNonInternMutableLeaf("nested")))


def test_interned_object_rejects_nested_non_intern_reference(db0_fixture):
    value = {"items": (MemoNonInternImmutableLeaf("nested"),)}

    with pytest.raises((RuntimeError, AttributeError), match="intern.*reference"):
        db0.materialized(MemoInternContainerHolder(value))
