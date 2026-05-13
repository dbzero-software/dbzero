# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from dataclasses import dataclass

import dbzero as db0

from .conftest import DB0_DIR


@db0.memo(protect_fields=True)
@dataclass
class MemoProtectedFieldsClass:
    name: str
    value: int


@db0.memo
@dataclass
class MemoUnprotectedFieldsClass:
    name: str
    value: int


def get_memo_class_object(obj):
    return db0.get_memo_class(obj).get_class()


def test_protect_fields_defaults_to_false(db0_fixture):
    obj = MemoUnprotectedFieldsClass("alpha", 1)
    assert get_memo_class_object(obj).get_type_flags()["protect_fields"] is False


def test_protect_fields_is_persisted_on_class(db0_fixture):
    obj = MemoProtectedFieldsClass("alpha", 1)
    flags = get_memo_class_object(obj).get_type_flags()
    assert flags["protect_fields"] is True
    assert flags["singleton"] is False
    assert flags["no_default_tags"] is False
    assert flags["immutable"] is False


def test_protect_fields_survives_redecoration_without_parameter(db0_fixture):
    @db0.memo(id="dbzero-software/dbzero/tests/protected-redecorated", protect_fields=True)
    @dataclass
    class ProtectedBefore:
        name: str

    obj = ProtectedBefore("alpha")
    obj_id = db0.uuid(obj)
    assert get_memo_class_object(obj).get_type_flags()["protect_fields"] is True
    db0.commit()

    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix")

    @db0.memo(id="dbzero-software/dbzero/tests/protected-redecorated")
    @dataclass
    class ProtectedAfter:
        name: str

    obj = db0.fetch(ProtectedAfter, obj_id)
    assert get_memo_class_object(obj).get_type_flags()["protect_fields"] is True


def test_explicit_false_does_not_unprotect_materialized_class(db0_fixture):
    @db0.memo(id="dbzero-software/dbzero/tests/protected-explicit-false", protect_fields=True)
    @dataclass
    class ProtectedBefore:
        name: str

    obj = ProtectedBefore("alpha")
    obj_id = db0.uuid(obj)
    assert get_memo_class_object(obj).get_type_flags()["protect_fields"] is True
    db0.commit()

    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix")

    @db0.memo(id="dbzero-software/dbzero/tests/protected-explicit-false", protect_fields=False)
    @dataclass
    class ProtectedAfter:
        name: str

    obj = db0.fetch(ProtectedAfter, obj_id)
    assert get_memo_class_object(obj).get_type_flags()["protect_fields"] is True


def test_protect_fields_can_be_enabled_after_class_materialization(db0_fixture):
    @db0.memo(id="dbzero-software/dbzero/tests/protected-enabled-later")
    @dataclass
    class ProtectedBefore:
        name: str

    obj = ProtectedBefore("alpha")
    obj_id = db0.uuid(obj)
    assert get_memo_class_object(obj).get_type_flags()["protect_fields"] is False
    db0.commit()

    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix")

    @db0.memo(id="dbzero-software/dbzero/tests/protected-enabled-later", protect_fields=True)
    @dataclass
    class ProtectedAfter:
        name: str

    obj = db0.fetch(ProtectedAfter, obj_id)
    assert get_memo_class_object(obj).get_type_flags()["protect_fields"] is True


def test_reset_protect_fields_clears_persisted_flag(db0_fixture):
    obj = MemoProtectedFieldsClass("alpha", 1)
    memo_class = get_memo_class_object(obj)
    assert memo_class.get_type_flags()["protect_fields"] is True

    memo_class.reset_protect_fields()
    assert memo_class.get_type_flags()["protect_fields"] is False
