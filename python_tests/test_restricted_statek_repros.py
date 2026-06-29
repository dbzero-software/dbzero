# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from dataclasses import dataclass, field
import gc
import os
import shutil
from typing import Optional

import pytest
import dbzero as db0

from .conftest import DB0_DIR


@db0.memo
@dataclass
class RestrictedDataclassWithPostInit:
    value: int
    initialized: bool = False

    def __post_init__(self):
        self.initialized = True


@db0.memo
@dataclass
class RestrictedMemoWithProperty:
    local_state: dict = field(default_factory=dict)

    @property
    def perm_ctx(self):
        return self.local_state.get("_PERM_CTX")


@db0.memo
@dataclass
class RestrictedMemoWithLeakyProperty:
    @property
    def class_object(self):
        return self.__class__


@db0.memo
@dataclass
class RestrictedMemoWithProtectedPostInitHelper:
    value: int
    synced: bool = False

    def __post_init__(self):
        self._sync_identity_hash_tag()

    def _sync_identity_hash_tag(self):
        self.synced = True


@db0.memo
@dataclass
class RestrictedMemoWithPostInitPublicFieldRead:
    role: str
    role_seen_in_post_init: Optional[str] = None

    def __post_init__(self):
        self.role_seen_in_post_init = self.role


@db0.memo
@dataclass
class RestrictedMemoWithProtectedMethodHelper:
    status: str = "READY"

    def set_status(self, status):
        self._set_status(status)

    def _set_status(self, status):
        self.status = status


@db0.memo
@dataclass
class RestrictedMemoWithProtectedPropertyHelper:
    metadata: dict = field(default_factory=dict)

    @property
    def model(self):
        return self._model_from_metadata()

    def _model_from_metadata(self):
        return self.metadata.get("MODEL")


@db0.memo
@dataclass
class RestrictedMemoWithLeakyProtectedHelper:
    def reveal_class(self):
        return self._class_object()

    def _class_object(self):
        return self.__class__


def _clean_db0_dir():
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    os.mkdir(DB0_DIR)


@pytest.fixture()
def db0_restricted_root():
    if "D" in db0.build_flags():
        db0.reset_test_params()
    _clean_db0_dir()
    yield
    gc.collect()
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


def test_restricted_memo_dataclass_post_init_runs_during_construction(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    db0.open("statek-post-init-repro")

    obj = RestrictedDataclassWithPostInit(123)

    assert obj.value == 123
    assert obj.initialized is True
    with pytest.raises(AttributeError):
        obj.__post_init__


def test_restricted_memo_public_property_can_be_read(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    db0.open("statek-property-repro")
    ctx = {"last_example_id": 1}
    obj = RestrictedMemoWithProperty(local_state={"_PERM_CTX": ctx})

    assert obj.perm_ctx == ctx


def test_restricted_memo_property_cannot_bypass_restricted_attribute_access(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    db0.open("statek-property-security-repro")
    obj = RestrictedMemoWithLeakyProperty()

    with pytest.raises(AttributeError):
        obj.class_object


def test_restricted_memo_post_init_can_call_protected_self_helper(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    db0.open("statek-post-init-protected-helper-repro")

    obj = RestrictedMemoWithProtectedPostInitHelper(123)

    assert obj.value == 123
    assert obj.synced is True
    with pytest.raises(AttributeError):
        obj._sync_identity_hash_tag


def test_restricted_memo_post_init_can_read_constructor_public_field(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    db0.open("statek-post-init-public-field-read-repro")

    obj = RestrictedMemoWithPostInitPublicFieldRead("sandbox-agent")

    assert obj.role == "sandbox-agent"
    assert obj.role_seen_in_post_init == "sandbox-agent"
    with pytest.raises(AttributeError):
        obj.__post_init__


def test_restricted_memo_public_method_can_call_protected_self_helper(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    db0.open("statek-method-protected-helper-repro")
    obj = RestrictedMemoWithProtectedMethodHelper()

    obj.set_status("STARTED")

    assert obj.status == "STARTED"
    with pytest.raises(AttributeError):
        obj._set_status


def test_restricted_memo_public_property_can_call_protected_self_helper(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    db0.open("statek-property-protected-helper-repro")
    obj = RestrictedMemoWithProtectedPropertyHelper(metadata={"MODEL": "test-model"})

    assert obj.model == "test-model"
    with pytest.raises(AttributeError):
        obj._model_from_metadata


def test_restricted_memo_protected_helper_cannot_leak_dunder_access(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    db0.open("statek-protected-helper-security-repro")
    obj = RestrictedMemoWithLeakyProtectedHelper()

    with pytest.raises(AttributeError):
        obj.reveal_class()
