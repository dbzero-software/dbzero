# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

from dataclasses import dataclass
import pickle

import pytest
import dbzero as db0


def test_fields_of_rejects_non_memo_type():
    with pytest.raises(TypeError, match="memo type"):
        db0.fields_of(object)


def test_fields_of_preserves_dataclass_class_attributes(db0_fixture):
    @db0.memo
    @dataclass
    class FieldRefDataclassTask:
        title: str
        priority: int = 0

    assert FieldRefDataclassTask.priority == 0
    assert not hasattr(FieldRefDataclassTask, "__fields__")

    ns = db0.fields_of(FieldRefDataclassTask)
    assert ns is db0.fields_of(FieldRefDataclassTask)
    assert ns.priority is ns.priority
    assert repr(ns.priority).endswith(".FieldRefDataclassTask.priority>")
    assert "priority" in dir(ns)

    with pytest.raises(AttributeError):
        ns.missing


def test_fields_of_inherits_declared_field_identity(db0_fixture):
    @db0.memo
    class FieldRefBase:
        def __init__(self, created_at):
            self.created_at = created_at

    @db0.memo
    class FieldRefDerived(FieldRefBase):
        def __init__(self, created_at, priority):
            super().__init__(created_at)
            self.priority = priority

    assert db0.fields_of(FieldRefDerived).created_at is db0.fields_of(FieldRefBase).created_at
    assert db0.fields_of(FieldRefDerived).priority is db0.fields_of(FieldRefDerived).priority


def test_fields_of_supports_explicit_dynamic_names(db0_fixture):
    @db0.memo
    class FieldRefDynamic:
        pass

    dynamic = db0.fields_of(FieldRefDynamic)["from"]
    assert dynamic == db0.fields_of(FieldRefDynamic)["from"]
    assert dynamic is db0.fields_of(FieldRefDynamic)["from"]

    with pytest.raises(TypeError):
        db0.fields_of(FieldRefDynamic)[123]

    with pytest.raises(Exception, match="Invalid persistent field name"):
        db0.fields_of(FieldRefDynamic)["_X__hidden"]


def test_field_ref_and_namespace_are_not_picklable(db0_fixture):
    @db0.memo
    class FieldRefPickle:
        def __init__(self, value):
            self.value = value

    ns = db0.fields_of(FieldRefPickle)
    with pytest.raises(TypeError):
        pickle.dumps(ns)
    with pytest.raises(TypeError):
        pickle.dumps(ns.value)


def test_index_of_accepts_field_ref(db0_fixture):
    @db0.memo
    @db0.indexed_fields("priority")
    class FieldRefIndexedTask:
        def __init__(self, name):
            self.name = name

    by_string = db0.index_of(FieldRefIndexedTask, "priority")
    by_ref = db0.index_of(db0.fields_of(FieldRefIndexedTask).priority)
    low = FieldRefIndexedTask("low")
    high = FieldRefIndexedTask("high")
    low.priority = 1
    high.priority = 5

    assert [item.name for item in db0.find(FieldRefIndexedTask, by_string.select(1, 1))] == ["low"]
    assert [item.name for item in db0.find(FieldRefIndexedTask, by_ref.select(5, 5))] == ["high"]

    with pytest.raises(Exception, match="does not accept a field_name"):
        db0.index_of(db0.fields_of(FieldRefIndexedTask).priority, "priority")
