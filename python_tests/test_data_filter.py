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
