# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from dataclasses import dataclass

import dbzero as db0

from .conftest import DB0_DIR


@db0.enum(values=["CREATE", "READ", "UPDATE", "DELETE"])
class FieldAccess:
    pass


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


@db0.memo(protect_fields=True)
class MemoProtectedDynamicFieldsClass:
    def __init__(self, count):
        for i in range(count):
            setattr(self, f"field_{i}", i)


@db0.memo(protect_fields=True)
class MemoProtectedManyInitFieldsClass:
    def __init__(self, count):
        values = (
            lambda i: i,
            lambda i: bool(i % 2),
            lambda i: f"value-{i}",
            lambda i: i / 10,
            lambda i: bytes([i % 256]),
            lambda i: (i, f"tuple-{i}"),
        )
        for i in range(count):
            setattr(self, f"init_field_{i}", values[i % len(values)](i))


def get_memo_class_object(obj):
    return db0.get_memo_class(obj).get_class()


def test_protect_fields_defaults_to_false(db0_fixture):
    obj = MemoUnprotectedFieldsClass("alpha", 1)
    assert get_memo_class_object(obj).get_type_flags()["protect_fields"] is False
    description = db0.describe(obj)
    assert description["protected_fields"] is False
    assert description["field_offset_range"] == 0


def test_protect_fields_is_persisted_on_class(db0_fixture):
    obj = MemoProtectedFieldsClass("alpha", 1)
    flags = get_memo_class_object(obj).get_type_flags()
    assert flags["protect_fields"] is True
    assert flags["singleton"] is False
    assert flags["no_default_tags"] is False
    assert flags["immutable"] is False
    description = db0.describe(obj)
    assert description["protected_fields"] is True
    assert description["field_offset_range"] > 0


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


def test_set_field_access_accepts_single_and_multiple_accounts(db0_fixture):
    MemoProtectedFieldsClass("alpha", 1)

    db0.set_field_access(MemoProtectedFieldsClass, 123, (FieldAccess.READ,), "name")
    db0.set_field_access(
        MemoProtectedFieldsClass,
        [123, 456],
        (FieldAccess.CREATE, FieldAccess.UPDATE),
        "name",
        "value",
    )


def test_set_field_access_accepts_empty_mode_to_clear(db0_fixture):
    MemoProtectedFieldsClass("alpha", 1)

    db0.set_field_access(MemoProtectedFieldsClass, 123, (), "name")


def test_set_field_access_requires_protected_class(db0_fixture):
    MemoUnprotectedFieldsClass("alpha", 1)

    try:
        db0.set_field_access(MemoUnprotectedFieldsClass, 123, (FieldAccess.READ,), "name")
    except Exception as exc:
        assert "protected fields" in str(exc)
    else:
        assert False, "set_field_access should fail for unprotected classes"


def test_set_field_access_accepts_unknown_field_names(db0_fixture):
    MemoProtectedFieldsClass("alpha", 1)

    db0.set_field_access(MemoProtectedFieldsClass, 123, (FieldAccess.READ,), "missing")


def test_get_field_access_returns_assigned_masks(db0_fixture):
    MemoProtectedFieldsClass("alpha", 1)
    db0.set_field_access(MemoProtectedFieldsClass, 123, (FieldAccess.READ,), "name")
    db0.set_field_access(
        MemoProtectedFieldsClass,
        123,
        (FieldAccess.CREATE, FieldAccess.UPDATE),
        "value",
    )

    result = dict(db0.get_field_access(MemoProtectedFieldsClass, 123))

    assert result["name"] == ("READ",)
    assert result["value"] == ("CREATE", "UPDATE")


def test_get_field_access_returns_empty_mask_after_clear(db0_fixture):
    MemoProtectedFieldsClass("alpha", 1)
    db0.set_field_access(MemoProtectedFieldsClass, 123, (), "name")

    result = dict(db0.get_field_access(MemoProtectedFieldsClass, 123))

    assert result["name"] == ()


def test_get_field_access_returns_predeclared_and_later_materialized_field(db0_fixture):
    obj = MemoProtectedFieldsClass("alpha", 1)
    expected = {
        "missing_a": ("READ",),
        "missing_b": ("CREATE", "UPDATE"),
        "missing_c": ("DELETE",),
    }
    db0.set_field_access(MemoProtectedFieldsClass, 123, (FieldAccess.READ,), "missing_a")
    db0.set_field_access(
        MemoProtectedFieldsClass,
        123,
        (FieldAccess.CREATE, FieldAccess.UPDATE),
        "missing_b",
    )
    db0.set_field_access(MemoProtectedFieldsClass, 123, (FieldAccess.DELETE,), "missing_c")

    predeclared = dict(db0.get_field_access(MemoProtectedFieldsClass, 123))
    for field_name, mask in expected.items():
        assert predeclared[field_name] == mask

    for index, field_name in enumerate(expected):
        setattr(obj, field_name, index)
        materialized = dict(db0.get_field_access(MemoProtectedFieldsClass, 123))
        for expected_field_name, mask in expected.items():
            assert materialized[expected_field_name] == mask


def test_get_field_access_returns_empty_for_unknown_account(db0_fixture):
    MemoProtectedFieldsClass("alpha", 1)

    assert list(db0.get_field_access(MemoProtectedFieldsClass, 999)) == []


def test_describe_field_offset_range_stays_constrained_for_many_fields(db0_fixture):
    field_count = 512
    total_field_count = field_count * 2
    obj = MemoProtectedDynamicFieldsClass(field_count)
    initial_range = db0.describe(obj)["field_offset_range"]

    assert db0.describe(obj)["protected_fields"] is True
    assert initial_range < field_count * 8

    for i in range(field_count, field_count * 2):
        setattr(obj, f"field_{i}", i)

    dynamic_range = db0.describe(obj)["field_offset_range"]
    assert dynamic_range < total_field_count * 2 + 128


def test_describe_field_offset_range_stays_constrained_for_many_init_fields_with_various_types(db0_fixture):
    field_count = 768
    obj = MemoProtectedManyInitFieldsClass(field_count)

    description = db0.describe(obj)

    assert description["protected_fields"] is True
    assert description["field_offset_range"] < field_count * 2 + 128


def test_describe_field_offset_range_counts_predeclared_access_fields(db0_fixture):
    obj = MemoProtectedDynamicFieldsClass(3)
    baseline_range = db0.describe(obj)["field_offset_range"]

    for i in range(120):
        db0.set_field_access(
            MemoProtectedDynamicFieldsClass,
            123,
            (FieldAccess.READ,),
            f"future_field_{i}",
        )

    predeclared_range = db0.describe(obj)["field_offset_range"]
    assert predeclared_range > baseline_range
    assert predeclared_range < 256

    for i in range(120):
        setattr(obj, f"future_field_{i}", i)

    materialized_range = db0.describe(obj)["field_offset_range"]
    assert materialized_range >= predeclared_range
    assert materialized_range < 120 * 2 + 128
