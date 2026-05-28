# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2026 DBZero Software sp. z o.o.

from contextvars import ContextVar
from dataclasses import dataclass

import pytest

import dbzero as db0
from .conftest import DB0_DIR


predicate = ContextVar("predicate")


@db0.memo
@dataclass
class InitDataFilterClass:
    value: str


@db0.memo(access_control=True)
@dataclass
class FilteredFindClass:
    value: str


@db0.memo(access_control=True)
class DynamicPrefixFilteredFindClass:
    def __init__(self, value: str, prefix=None):
        db0.set_prefix(self, prefix)
        self.value = value


@db0.memo
@dataclass
class FilteredFindPublicClass:
    value: str


@db0.memo
@dataclass
class FilteredFindBaseClass:
    value: str


@db0.memo(access_control=True)
@dataclass
class FilteredFindDerivedClass(FilteredFindBaseClass):
    extra: str


@db0.memo
class FilteredReferenceHolder:
    def __init__(self, payload):
        self.payload = payload


@db0.memo(immutable=True)
class FilteredImmutableReferenceHolder:
    def __init__(self, payload):
        self.payload = payload


def test_init_data_filter_prefix_scoped_lifecycle(db0_fixture):
    current_prefix = db0.get_current_prefix()

    db0._init_data_filter(predicate, prefix=current_prefix, mode="DEBUG")

    db0._init_data_filter(predicate, prefix=current_prefix.name, mode="DEBUG")

    db0.open("data-filter-extra-prefix")
    db0._init_data_filter(
        predicate,
        prefix=["data-filter-extra-prefix"],
        mode="DEBUG",
    )


def test_init_data_filter_general_scope_lifecycle(db0_fixture):
    db0._init_data_filter(predicate)

    db0._init_data_filter(predicate, mode="RELEASE")

    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_filter(ContextVar("other_general_predicate"))

    db0.open("data-filter-general-prefix")
    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_filter(
            ContextVar("other_general_prefix_predicate"),
            prefix="data-filter-general-prefix",
        )


def test_init_data_filter_allows_prefix_before_open(db0_fixture):
    prefix_name = "not-yet-opened-data-filter-prefix"
    db0._init_data_filter(predicate, prefix=prefix_name, mode="DEBUG")
    db0.open(prefix_name)

    stats = db0.get_prefix_stats(prefix=prefix_name)
    assert stats["data_filter"]["enabled"] is True

    db0.open("readonly-data-filter-prefix")
    db0.close("readonly-data-filter-prefix")
    db0.open("readonly-data-filter-prefix", "r")
    db0._init_data_filter(predicate, prefix="readonly-data-filter-prefix")


def test_init_data_filter_rejects_parameter_changes(db0_fixture):
    db0._init_data_filter(
        predicate,
        prefix=db0.get_current_prefix(),
        mode="DEBUG",
    )

    other_predicate = ContextVar("other_predicate")
    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_filter(
            other_predicate,
            prefix=db0.get_current_prefix(),
            mode="DEBUG",
        )

    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_filter(
            predicate,
            prefix=db0.get_current_prefix(),
            mode="RELEASE",
        )


def test_init_data_filter_defaults_mode_to_release(db0_fixture):
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    db0._init_data_filter(
        predicate,
        prefix=db0.get_current_prefix(),
        mode="RELEASE",
    )

    db0._init_data_filter(
        predicate,
        prefix=db0.get_current_prefix(),
        mode=None,
    )

    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_filter(
            predicate,
            prefix=db0.get_current_prefix(),
            mode="DEBUG",
        )


def test_init_data_filter_binding_survives_prefix_reopen(db0_fixture):
    prefix_name = db0.get_current_prefix().name

    db0._init_data_filter(predicate, prefix=prefix_name)
    db0.close(prefix_name)
    db0.open(prefix_name)

    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_filter(
            ContextVar("reopened_prefix_predicate"),
            prefix=prefix_name,
        )


def test_init_data_filter_allows_different_bindings_for_different_prefixes(db0_fixture):
    db0.open("first-data-filter-binding")
    db0._init_data_filter(predicate, prefix="first-data-filter-binding", mode="DEBUG")

    other_predicate = ContextVar("different_prefix_predicate")
    db0.open("different-data-filter-binding")
    db0._init_data_filter(
        other_predicate,
        prefix="different-data-filter-binding",
        mode="RELEASE",
    )


def test_init_can_initialize_workspace_data_filter(db0_fixture):
    db0.close()
    init_predicate = ContextVar("init_workspace_data_filter_predicate")

    db0.init(
        DB0_DIR,
        data_filter={
            "context_var": init_predicate,
            "mode": "DEBUG",
        },
    )
    db0.open("init-workspace-data-filter")
    init_predicate.set(None)

    obj = InitDataFilterClass("visible")

    assert obj.value == "visible"


def test_predicate_can_be_built_without_data_filter_enabled(db0_fixture):
    obj = FilteredFindPublicClass("allowed")
    db0.tags(obj).add("grant")

    pred = db0.predicate("grant")

    assert [item.value for item in db0.find(FilteredFindPublicClass, pred)] == ["allowed"]


def test_predicate_survives_serialization(db0_fixture):
    obj = FilteredFindPublicClass("allowed")
    db0.tags(obj).add("grant")

    pred = db0.deserialize(db0.serialize(db0.predicate("grant")))

    with pytest.raises(PermissionError):
        list(pred)
    assert [item.value for item in db0.find(FilteredFindPublicClass, pred)] == ["allowed"]


def test_predicate_can_be_built_after_data_filter_enabled(db0_fixture):
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix(), mode="DEBUG")

    pred = db0.predicate("grant")

    with pytest.raises(PermissionError):
        list(pred)


def test_predicate_blocks_direct_data_access(db0_fixture):
    pred = db0.predicate("grant")

    with pytest.raises(PermissionError):
        iter(pred)
    with pytest.raises(PermissionError):
        len(pred)
    with pytest.raises(PermissionError):
        bool(pred)
    with pytest.raises(PermissionError):
        pred[0]
    with pytest.raises(PermissionError):
        pred[:1]


def test_data_filter_rejects_typeless_find(db0_fixture):
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix(), mode="DEBUG")

    with pytest.raises(PermissionError):
        db0.find("grant")


def test_access_controlled_find_requires_data_filter(db0_fixture):
    FilteredFindClass("visible")

    with pytest.raises(PermissionError):
        db0.find(FilteredFindClass)


def test_data_filter_release_mode_requires_predicate(db0_fixture):
    FilteredFindClass("visible")
    predicate.set(None)
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    with pytest.raises(PermissionError):
        db0.find(FilteredFindClass)


def test_data_filter_debug_mode_allows_null_predicate(db0_fixture):
    obj = FilteredFindClass("visible")
    predicate.set(None)
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix(), mode="DEBUG")

    assert list(db0.find(FilteredFindClass)) == [obj]


def test_data_filter_predicate_filters_access_controlled_find(db0_fixture):
    allowed = FilteredFindClass("allowed")
    denied = FilteredFindClass("denied")
    db0.tags(allowed).add(["visible", "grant"])
    db0.tags(denied).add("visible")

    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    assert list(db0.find(FilteredFindClass, "visible")) == [allowed]


def test_data_filter_predicate_refreshes_after_matching_objects_are_committed(db0_fixture):
    initial = FilteredFindClass("initial")
    db0.tags(initial).add(["visible", "grant"])
    db0.commit()

    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    denied = FilteredFindClass("denied")
    db0.tags(denied).add("visible")
    assert list(db0.find(FilteredFindClass, "visible")) == [initial]

    later = FilteredFindClass("later")
    db0.tags(later).add(["visible", "grant"])
    db0.commit()

    assert list(db0.find(FilteredFindClass, "visible")) == [later, initial]


@pytest.mark.skip(reason="TODO: preserve predicate expressions that initially resolve to no results")
def test_data_filter_predicate_created_before_tag_exists_refreshes_after_commit(db0_fixture):
    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    allowed = FilteredFindClass("allowed")
    denied = FilteredFindClass("denied")
    db0.tags(allowed).add(["visible", "grant"])
    db0.tags(denied).add("visible")
    db0.commit()

    assert list(db0.find(FilteredFindClass, "visible")) == [allowed]


def test_data_filter_requires_predicate_object(db0_fixture):
    FilteredFindClass("visible")
    predicate.set(db0.find(FilteredFindPublicClass))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    with pytest.raises(RuntimeError, match="db0.predicate"):
        db0.find(FilteredFindClass)


def test_data_filter_filters_access_controlled_derived_from_base_query(db0_fixture):
    allowed = FilteredFindDerivedClass("allowed", "x")
    denied = FilteredFindDerivedClass("denied", "y")
    db0.tags(allowed).add(["visible", "grant"])
    db0.tags(denied).add("visible")

    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    assert list(db0.find(FilteredFindBaseClass, "visible")) == [allowed]


def test_data_filter_prefix_scope_only_filters_configured_prefix(db0_fixture):
    px_name = db0.get_current_prefix().name
    other_prefix = "unfiltered-data-filter-find-prefix"

    filtered = DynamicPrefixFilteredFindClass("filtered", prefix=px_name)
    db0.tags(filtered).add("visible")
    db0.open(other_prefix)
    unfiltered = DynamicPrefixFilteredFindClass("unfiltered", prefix=other_prefix)
    db0.tags(unfiltered).add("visible")

    predicate.set(db0.predicate("grant", prefix=px_name))
    db0._init_data_filter(predicate, prefix=px_name)

    assert list(db0.find(DynamicPrefixFilteredFindClass, "visible", prefix=px_name)) == []
    assert list(db0.find(DynamicPrefixFilteredFindClass, "visible", prefix=other_prefix)) == [unfiltered]


def test_data_filter_fetch_requires_data_filter_for_access_controlled_type(db0_fixture):
    obj = FilteredFindClass("visible")

    with pytest.raises(RuntimeError, match="Invalid UUID or object has been deleted"):
        db0.fetch(db0.uuid(obj))


def test_data_filter_fetch_release_mode_requires_predicate(db0_fixture):
    obj = FilteredFindClass("visible")
    predicate.set(None)
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    with pytest.raises(RuntimeError, match="Invalid UUID or object has been deleted"):
        db0.fetch(db0.uuid(obj))


def test_data_filter_fetch_debug_mode_allows_null_predicate(db0_fixture):
    obj = FilteredFindClass("visible")
    predicate.set(None)
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix(), mode="DEBUG")

    assert db0.fetch(db0.uuid(obj)) is obj


def test_data_filter_predicate_filters_access_controlled_fetch(db0_fixture):
    allowed = FilteredFindClass("allowed")
    denied = FilteredFindClass("denied")
    db0.tags(allowed).add("grant")

    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    assert db0.fetch(db0.uuid(allowed)) is allowed
    with pytest.raises(RuntimeError, match="Invalid UUID or object has been deleted"):
        db0.fetch(db0.uuid(denied))


def test_data_filter_predicate_filters_access_controlled_snapshot_fetch(db0_fixture):
    allowed = FilteredFindClass("snapshot-allowed")
    denied = FilteredFindClass("snapshot-denied")
    db0.tags(allowed).add("grant")
    db0.commit()

    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())
    snap = db0.snapshot()

    assert snap.fetch(db0.uuid(allowed)).value == "snapshot-allowed"
    with pytest.raises(RuntimeError, match="Invalid UUID or object has been deleted"):
        snap.fetch(db0.uuid(denied))


def test_data_filter_fetch_does_not_filter_public_type(db0_fixture):
    obj = FilteredFindPublicClass("public")
    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    assert db0.fetch(db0.uuid(obj)) is obj


def test_data_filter_prefix_scope_only_filters_configured_prefix_fetch(db0_fixture):
    px_name = db0.get_current_prefix().name
    other_prefix = "unfiltered-data-filter-fetch-prefix"

    filtered = DynamicPrefixFilteredFindClass("filtered", prefix=px_name)
    db0.open(other_prefix)
    unfiltered = DynamicPrefixFilteredFindClass("unfiltered", prefix=other_prefix)

    predicate.set(db0.predicate("grant", prefix=px_name))
    db0._init_data_filter(predicate, prefix=px_name)

    with pytest.raises(RuntimeError, match="Invalid UUID or object has been deleted"):
        db0.fetch(db0.uuid(filtered))
    assert db0.fetch(db0.uuid(unfiltered)) is unfiltered


def test_data_filter_predicate_filters_mutable_memo_reference(db0_fixture):
    allowed = FilteredFindClass("allowed")
    denied = FilteredFindClass("denied")
    db0.tags(allowed).add("grant")
    allowed_holder = FilteredReferenceHolder(allowed)
    denied_holder = FilteredReferenceHolder(denied)

    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    assert allowed_holder.payload is allowed
    with pytest.raises(PermissionError):
        denied_holder.payload


def test_data_filter_predicate_filters_reference_from_immutable_memo(db0_fixture):
    allowed = FilteredFindClass("immutable-source-allowed")
    denied = FilteredFindClass("immutable-source-denied")
    db0.tags(allowed).add("grant")
    allowed_holder = FilteredImmutableReferenceHolder(allowed)
    denied_holder = FilteredImmutableReferenceHolder(denied)

    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    assert allowed_holder.payload is allowed
    with pytest.raises(PermissionError):
        denied_holder.payload
    db0.commit()


def test_data_filter_predicate_filters_references_from_collections(db0_fixture):
    allowed = FilteredFindClass("collection-allowed")
    denied = FilteredFindClass("collection-denied")
    db0.tags(allowed).add("grant")
    list_holder = FilteredReferenceHolder(db0.list([allowed, denied]))
    set_holder = FilteredReferenceHolder(db0.set([denied]))
    dict_holder = FilteredReferenceHolder(db0.dict({"allowed": allowed, "denied": denied}))

    predicate.set(db0.predicate("grant"))
    db0._init_data_filter(predicate, prefix=db0.get_current_prefix())

    assert list_holder.payload[0] is allowed
    with pytest.raises(PermissionError):
        list_holder.payload[1]
    with pytest.raises(PermissionError):
        next(iter(set_holder.payload))
    assert dict_holder.payload["allowed"] is allowed
    with pytest.raises(PermissionError):
        dict_holder.payload["denied"]


def test_init_can_initialize_prefix_data_filter_after_opening_prefix(db0_fixture):
    db0.close()
    init_predicate = ContextVar("init_prefix_data_filter_predicate")

    db0.init(
        DB0_DIR,
        prefix="init-prefix-data-filter",
        data_filter={
            "context_var": init_predicate,
            "prefix": "init-prefix-data-filter",
            "mode": "DEBUG",
        },
    )
    init_predicate.set(None)

    obj = InitDataFilterClass("visible")

    assert obj.value == "visible"
