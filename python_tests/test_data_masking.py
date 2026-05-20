# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from contextvars import ContextVar
from dataclasses import dataclass

import pytest

import dbzero as db0
from .conftest import DB0_DIR


account_id = ContextVar("account_id")
missing_value = object()


@db0.memo(protect_fields=True)
@dataclass
class InitDataMaskingProtectedClass:
    value: str


def test_init_data_masking_prefix_scoped_lifecycle(db0_fixture):
    current_prefix = db0.get_current_prefix()

    db0._init_data_masking(
        account_id,
        prefix=current_prefix,
        missing_value_placeholder=missing_value,
        mode="DEBUG",
    )

    db0._init_data_masking(
        account_id,
        prefix=current_prefix.name,
        missing_value_placeholder=missing_value,
        mode="DEBUG",
    )

    db0.open("data-masking-extra-prefix")
    db0._init_data_masking(
        account_id,
        prefix=["data-masking-extra-prefix"],
        missing_value_placeholder=missing_value,
        mode="DEBUG",
    )


def test_init_data_masking_general_scope_lifecycle(db0_fixture):
    db0._init_data_masking(account_id)

    db0._init_data_masking(account_id, mode="RELEASE")

    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_masking(ContextVar("other_general_account_id"))

    db0.open("data-masking-general-prefix")
    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_masking(
            ContextVar("other_general_prefix_account_id"),
            prefix="data-masking-general-prefix",
        )


def test_init_data_masking_requires_open_prefix(db0_fixture):
    with pytest.raises(ValueError, match="open"):
        db0._init_data_masking(account_id, prefix="not-opened")

    db0.open("readonly-data-masking-prefix")
    db0.close("readonly-data-masking-prefix")
    db0.open("readonly-data-masking-prefix", "r")
    db0._init_data_masking(account_id, prefix="readonly-data-masking-prefix")


def test_init_data_masking_rejects_parameter_changes(db0_fixture):
    db0._init_data_masking(
        account_id,
        prefix=db0.get_current_prefix(),
        missing_value_placeholder=missing_value,
        mode="DEBUG",
    )

    other_account_id = ContextVar("other_account_id")
    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_masking(
            other_account_id,
            prefix=db0.get_current_prefix(),
            missing_value_placeholder=missing_value,
            mode="DEBUG",
        )

    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_masking(
            account_id,
            prefix=db0.get_current_prefix(),
            missing_value_placeholder=missing_value,
            mode="RELEASE",
        )

    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_masking(
            account_id,
            prefix=db0.get_current_prefix(),
            missing_value_placeholder=object(),
            mode="DEBUG",
        )


def test_init_data_masking_defaults_mode_to_release(db0_fixture):
    db0._init_data_masking(account_id, prefix=db0.get_current_prefix())

    db0._init_data_masking(
        account_id,
        prefix=db0.get_current_prefix(),
        mode="RELEASE",
    )

    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_masking(
            account_id,
            prefix=db0.get_current_prefix(),
            mode="DEBUG",
        )


def test_init_data_masking_binding_survives_prefix_reopen(db0_fixture):
    prefix_name = db0.get_current_prefix().name

    db0._init_data_masking(account_id, prefix=prefix_name)
    db0.close(prefix_name)
    db0.open(prefix_name)

    with pytest.raises(RuntimeError, match="binding"):
        db0._init_data_masking(
            ContextVar("reopened_prefix_account_id"),
            prefix=prefix_name,
        )


def test_init_data_masking_allows_different_bindings_for_different_prefixes(db0_fixture):
    db0.open("first-data-masking-binding")
    db0._init_data_masking(account_id, prefix="first-data-masking-binding", mode="DEBUG")

    other_account_id = ContextVar("different_prefix_account_id")
    db0.open("different-data-masking-binding")
    db0._init_data_masking(
        other_account_id,
        prefix="different-data-masking-binding",
        missing_value_placeholder=object(),
        mode="RELEASE",
    )


def test_init_can_initialize_workspace_data_masking(db0_fixture):
    db0.close()
    init_account_id = ContextVar("init_workspace_data_masking_account_id")

    db0.init(
        DB0_DIR,
        data_masking={
            "context_var": init_account_id,
            "mode": "DEBUG",
        },
    )
    db0.open("init-workspace-data-masking")
    init_account_id.set(-2)

    obj = InitDataMaskingProtectedClass("visible")

    assert obj.value == "visible"


def test_init_can_initialize_prefix_data_masking_after_opening_prefix(db0_fixture):
    db0.close()
    init_account_id = ContextVar("init_prefix_data_masking_account_id")

    db0.init(
        DB0_DIR,
        prefix="init-prefix-data-masking",
        data_masking={
            "context_var": init_account_id,
            "prefix": "init-prefix-data-masking",
            "mode": "DEBUG",
        },
    )
    init_account_id.set(-2)

    obj = InitDataMaskingProtectedClass("visible")

    assert obj.value == "visible"
