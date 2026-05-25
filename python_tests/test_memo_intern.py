# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import gc
from dataclasses import dataclass

import pytest
import dbzero as db0

from .conftest import DB0_DIR


def get_memo_class_object(obj):
    return db0.get_memo_class(obj).get_class()


@db0.memo(immutable=True, intern=True)
@dataclass
class MemoInternLeaf:
    name: str


@db0.memo(immutable=True, intern=True)
@dataclass
class MemoInternLeafSibling:
    name: str


@db0.memo(immutable=True)
@dataclass
class MemoNonInternImmutableLeaf:
    name: str


@db0.memo(immutable=True, no_cache=True)
@dataclass
class MemoNoCacheImmutableLeaf:
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


@db0.memo(immutable=True)
class MemoImmutableHolder:
    def __init__(self, value):
        self.value = value


@db0.memo(immutable=True, intern=True)
class MemoInternComposite:
    def __init__(self, name, count, payload):
        self.name = name
        self.count = count
        self.payload = payload


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


@pytest.mark.xfail(
    raises=db0.ReferenceError,
    strict=True,
    reason="intern content currently decodes short OBJECT_REF values as UniqueAddress",
)
def test_interned_object_reuses_materialized_reference(db0_fixture):
    leaf = db0.materialized(MemoInternLeaf("materialized reference"))
    leaf_uuid = db0.uuid(leaf)

    holder = db0.materialized(MemoInternHolder(leaf))
    second = db0.materialized(MemoInternLeaf("materialized reference"))

    assert db0.uuid(holder.value) == leaf_uuid
    assert db0.uuid(second) == leaf_uuid
    assert holder.value.name == "materialized reference"


def test_embedded_interned_object_reuses_embedded_instance(db0_fixture):
    leaf = MemoInternLeaf("embedded")
    holder = db0.materialized(MemoInternHolder(leaf))
    db0.clear_cache()
    second = db0.materialized(MemoInternLeaf("embedded"))

    assert db0.uuid(leaf) == db0.uuid(holder.value)
    assert db0.uuid(second) == db0.uuid(leaf)
    assert leaf.name == "embedded"
    assert second.name == "embedded"


def test_fetch_embedded_object_reuses_lang_cache_entry(db0_fixture):
    leaf = MemoInternLeaf("embedded cache")
    holder = db0.materialized(MemoInternHolder(leaf))
    leaf_uuid = db0.uuid(holder.value)
    db0.tags(holder).add("keep-embedded-cache")
    db0.clear_cache()

    first = db0.fetch(leaf_uuid, MemoInternLeaf)
    second = db0.fetch(leaf_uuid, MemoInternLeaf)

    assert second is first
    assert second.name == "embedded cache"


def test_fetch_no_cache_embedded_object_is_not_added_to_lang_cache(db0_fixture):
    holder = db0.materialized(MemoImmutableHolder(MemoNoCacheImmutableLeaf("embedded no-cache")))
    leaf_uuid = db0.uuid(holder.value)
    db0.tags(holder).add("keep-embedded-no-cache")
    db0.clear_cache()

    first = db0.fetch(leaf_uuid, MemoNoCacheImmutableLeaf)
    second = db0.fetch(leaf_uuid, MemoNoCacheImmutableLeaf)

    assert second is not first
    assert first.name == "embedded no-cache"
    assert second.name == "embedded no-cache"


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


@pytest.mark.parametrize(
    ("make_values", "extract_value"),
    [
        pytest.param(lambda leaf: ("prefix", leaf), lambda values: values[1], id="tuple"),
        pytest.param(lambda leaf: ["prefix", leaf], lambda values: values[1], id="list"),
        pytest.param(
            lambda leaf: {"marker", leaf},
            lambda values: next(value for value in values if isinstance(value, MemoInternLeaf)),
            id="set",
        ),
        pytest.param(lambda leaf: {"child": leaf}, lambda values: values["child"], id="dict-value"),
        pytest.param(lambda leaf: {leaf: "child"}, lambda values: next(iter(values.keys())), id="dict-key"),
    ],
)
def test_embedded_interned_object_inside_container_reuses_embedded_instance(
    db0_fixture, make_values, extract_value
):
    leaf = MemoInternLeaf("container embedded")
    holder = db0.materialized(MemoInternContainerHolder(make_values(leaf)))
    second = db0.materialized(MemoInternLeaf("container embedded"))

    embedded_leaf = extract_value(holder.values)
    assert db0.uuid(embedded_leaf) == db0.uuid(leaf)
    assert db0.uuid(second) == db0.uuid(leaf)
    assert second.name == "container embedded"


def test_standalone_interned_object_reuses_existing_instance(db0_fixture):
    first = db0.materialized(MemoInternLeaf("dedupe"))
    db0.clear_cache()
    second = db0.materialized(MemoInternLeaf("dedupe"))

    assert db0.uuid(second) == db0.uuid(first)
    assert second.name == "dedupe"


def test_standalone_intern_lookup_uses_bound_type(db0_fixture):
    first = db0.materialized(MemoInternLeaf("same-content-bound-type"))
    sibling = db0.materialized(MemoInternLeafSibling("same-content-bound-type"))
    db0.clear_cache()
    second = db0.materialized(MemoInternLeaf("same-content-bound-type"))
    second_sibling = db0.materialized(MemoInternLeafSibling("same-content-bound-type"))

    assert db0.uuid(second) == db0.uuid(first)
    assert db0.uuid(second_sibling) == db0.uuid(sibling)
    assert db0.uuid(second) != db0.uuid(second_sibling)
    assert db0.get_type_stats(MemoInternLeaf)["content_index"]["size"] == 1
    assert db0.get_type_stats(MemoInternLeafSibling)["content_index"]["size"] == 1


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


def test_standalone_interned_object_reuses_after_close_and_reopen(db0_fixture):
    first = db0.materialized(MemoInternLeaf("reopened"))
    first_uuid = db0.uuid(first)
    db0.tags(first).add("keep-reopened-intern")
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    fetched = db0.fetch(first_uuid, MemoInternLeaf)
    second = db0.materialized(MemoInternLeaf("reopened"))

    assert db0.uuid(fetched) == first_uuid
    assert db0.uuid(second) == first_uuid
    assert second.name == "reopened"


def test_composite_interned_object_reuses_equivalent_content(db0_fixture):
    first = db0.materialized(MemoInternComposite(
        "composite", 7, {"items": ("alpha", 1), "flags": {"x", "y"}}
    ))
    second = db0.materialized(MemoInternComposite(
        "composite", 7, {"flags": {"y", "x"}, "items": ("alpha", 1)}
    ))
    different = db0.materialized(MemoInternComposite(
        "composite", 8, {"items": ("alpha", 1), "flags": {"x", "y"}}
    ))

    assert db0.uuid(second) == db0.uuid(first)
    assert db0.uuid(different) != db0.uuid(first)
    assert second.name == "composite"
    assert second.count == 7


def test_many_interned_materializations_reuse_root_and_embedded_candidates(db0_fixture):
    canonical_uuids = {}
    canonical_objects = []
    holders = []

    for index in range(128):
        name = f"bulk-{index % 16}"
        if name not in canonical_uuids and index % 2 == 0:
            leaf = MemoInternLeaf(name)
            holder = db0.materialized(MemoInternHolder(leaf))
            db0.tags(holder).add(f"keep-bulk-holder-{name}")
            holders.append(holder)
            canonical_uuids[name] = db0.uuid(leaf)
        else:
            leaf = db0.materialized(MemoInternLeaf(name))
            canonical_uuids.setdefault(name, db0.uuid(leaf))
            if len(canonical_objects) < len(canonical_uuids):
                db0.tags(leaf).add(f"keep-bulk-leaf-{name}")
                canonical_objects.append(leaf)
            assert db0.uuid(leaf) == canonical_uuids[name]

    assert len(canonical_uuids) == 16
    assert len(set(canonical_uuids.values())) == 16
    assert len(holders) == 8

    db0.commit()
    for index in range(128):
        name = f"bulk-{index % 16}"
        leaf = db0.materialized(MemoInternLeaf(name))

        assert db0.uuid(leaf) == canonical_uuids[name]
        assert leaf.name == name


def test_dropped_standalone_interned_object_is_not_reused(db0_fixture):
    obj = db0.materialized(MemoInternLeaf("dropped standalone"))
    old_uuid = db0.uuid(obj)
    assert db0._check_interned(old_uuid, MemoInternLeaf)
    assert db0.get_type_stats(MemoInternLeaf)["content_index"]["size"] == 1
    del obj
    gc.collect()
    db0.commit()

    assert not db0.exists(old_uuid)
    assert not db0._check_interned(old_uuid, MemoInternLeaf)
    assert db0.get_type_stats(MemoInternLeaf)["content_index"]["size"] == 0
    with pytest.raises(Exception):
        db0.fetch(old_uuid, MemoInternLeaf)

    replacement = db0.materialized(MemoInternLeaf("dropped standalone"))

    assert replacement.name == "dropped standalone"
    assert db0.exists(db0.uuid(replacement))
    assert db0._check_interned(db0.uuid(replacement), MemoInternLeaf)


def test_dropped_embedded_interned_object_is_not_reused(db0_fixture):
    leaf = MemoInternLeaf("dropped embedded")
    holder = db0.materialized(MemoInternHolder(leaf))
    leaf_uuid = db0.uuid(leaf)
    holder_uuid = db0.uuid(holder)
    assert db0._check_interned(leaf_uuid, MemoInternLeaf)
    assert db0.get_type_stats(MemoInternLeaf)["content_index"]["size"] == 1
    del leaf, holder
    gc.collect()
    db0.commit()

    assert not db0.exists(holder_uuid)
    assert not db0.exists(leaf_uuid)
    assert not db0._check_interned(leaf_uuid, MemoInternLeaf)
    assert db0.get_type_stats(MemoInternLeaf)["content_index"]["size"] == 0

    replacement = db0.materialized(MemoInternLeaf("dropped embedded"))

    assert replacement.name == "dropped embedded"
    assert db0.uuid(replacement) != leaf_uuid
    assert db0.exists(db0.uuid(replacement))


@pytest.mark.parametrize(
    "make_values",
    [
        pytest.param(lambda leaf: ("prefix", leaf), id="tuple"),
        pytest.param(lambda leaf: [leaf, "suffix"], id="list"),
        pytest.param(lambda leaf: {"child": leaf}, id="dict-value"),
    ],
)
def test_dropped_container_embedded_interned_object_is_not_reused(db0_fixture, make_values):
    leaf = MemoInternLeaf("dropped container embedded")
    holder = db0.materialized(MemoInternContainerHolder(make_values(leaf)))
    leaf_uuid = db0.uuid(leaf)
    holder_uuid = db0.uuid(holder)
    assert db0._check_interned(leaf_uuid, MemoInternLeaf)
    assert db0.get_type_stats(MemoInternLeaf)["content_index"]["size"] == 1
    del leaf, holder
    gc.collect()
    db0.commit()

    assert not db0.exists(holder_uuid)
    assert not db0.exists(leaf_uuid)
    assert not db0._check_interned(leaf_uuid, MemoInternLeaf)
    assert db0.get_type_stats(MemoInternLeaf)["content_index"]["size"] == 0

    replacement = db0.materialized(MemoInternLeaf("dropped container embedded"))

    assert replacement.name == "dropped container embedded"
    assert db0.uuid(replacement) != leaf_uuid
    assert db0.exists(db0.uuid(replacement))


def test_dropped_duplicate_intern_candidate_does_not_hide_live_candidate(db0_fixture):
    live = db0.materialized(MemoInternLeaf("live duplicate"))
    live_uuid = db0.uuid(live)
    db0.tags(live).add("keep-live-duplicate")

    duplicate_leaf = MemoInternLeaf("live duplicate")
    holder = db0.materialized(MemoInternHolder(duplicate_leaf))
    duplicate_uuid = db0.uuid(duplicate_leaf)
    holder_uuid = db0.uuid(holder)
    assert duplicate_uuid != live_uuid
    assert db0._check_interned(live_uuid, MemoInternLeaf)
    assert db0._check_interned(duplicate_uuid, MemoInternLeaf)
    assert db0.get_type_stats(MemoInternLeaf)["content_index"]["size"] == 2

    del duplicate_leaf, holder
    gc.collect()
    db0.commit()

    assert db0.exists(live_uuid)
    assert not db0.exists(holder_uuid)
    assert not db0.exists(duplicate_uuid)
    assert db0._check_interned(live_uuid, MemoInternLeaf)
    assert not db0._check_interned(duplicate_uuid, MemoInternLeaf)
    assert db0.get_type_stats(MemoInternLeaf)["content_index"]["size"] == 1

    replacement = db0.materialized(MemoInternLeaf("live duplicate"))

    assert db0.uuid(replacement) == live_uuid
    assert replacement.name == "live duplicate"


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
