# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from dataclasses import dataclass
from .conftest import DB0_DIR


@db0.memo
class MemoSimple:
    def __init__(self, value, name):
        self.value = value
        self.name = name


@db0.memo
class MemoWithMethod:
    def __init__(self, value):
        self.value = value

    def get_value(self):
        return self.value

    def double(self):
        return self.value * 2


@db0.memo
class MemoConditional:
    def __init__(self, value, include_extra=False):
        self.value = value
        if include_extra:
            self.extra = "extra"


@db0.memo
class MemoDynamic:
    def __init__(self, **kwargs):
        for key, val in kwargs.items():
            setattr(self, key, val)


@db0.memo
class MemoWithDeleted:
    def __init__(self, value):
        self.value = value
        self.temp = "temporary"
        del self.temp


@db0.memo(singleton=True)
class MemoSingleton:
    def __init__(self, value):
        self.value = value
        self.count = 0


@db0.memo
class MemoEmpty:
    def __init__(self):
        pass


@db0.memo
class MemoBase:
    def __init__(self, base_val):
        self.base_val = base_val


@db0.memo
class MemoDerived(MemoBase):
    def __init__(self, base_val, derived_val):
        super().__init__(base_val)
        self.derived_val = derived_val


@db0.memo
class MemoWithNone:
    def __init__(self):
        self.a = None
        self.b = 0
        self.c = ""


@db0.memo
class MemoWithComplexFields:
    def __init__(self, items, mapping, nested):
        self.items = items
        self.mapping = mapping
        self.nested = nested


@db0.memo
class MemoWithNonPersistent:
    def __init__(self, value):
        self.value = value
        self._X__cache = "not persistent"


@db0.memo
class MemoWithProperty:
    def __init__(self, value):
        self.__value = value

    @property
    def value(self):
        return self.__value


@db0.memo(immutable=True, no_default_tags=True)
@dataclass
class MemoImmutable:
    data: str
    count: int = 0


# --- dir() tests ---

def test_dir_includes_init_vars(db0_fixture):
    obj = MemoSimple(42, "hello")
    result = dir(obj)
    assert "value" in result
    assert "name" in result


def test_dir_includes_methods(db0_fixture):
    obj = MemoWithMethod(10)
    result = dir(obj)
    assert "get_value" in result
    assert "double" in result


def test_dir_includes_init_vars_and_methods(db0_fixture):
    obj = MemoWithMethod(10)
    result = dir(obj)
    assert "value" in result
    assert "get_value" in result
    assert "double" in result


def test_dir_excludes_deleted_field(db0_fixture):
    obj = MemoWithDeleted(5)
    result = dir(obj)
    assert "value" in result
    assert "temp" not in result


def test_dir_excludes_field_deleted_after_init(db0_fixture):
    obj = MemoSimple(1, "test")
    del obj.name
    result = dir(obj)
    assert "value" in result
    assert "name" not in result


def test_dir_reflects_dynamically_assigned_attrs(db0_fixture):
    obj = MemoDynamic(x=1, y=2, z=3)
    result = dir(obj)
    assert "x" in result
    assert "y" in result
    assert "z" in result


def test_dir_for_conditional_field_present(db0_fixture):
    obj = MemoConditional(10, include_extra=True)
    result = dir(obj)
    assert "value" in result
    assert "extra" in result


def test_dir_for_conditional_field_absent(db0_fixture):
    obj = MemoConditional(10, include_extra=False)
    result = dir(obj)
    assert "value" in result
    assert "extra" not in result


def test_dir_after_new_attr_assigned(db0_fixture):
    obj = MemoSimple(1, "a")
    obj.new_field = 99
    result = dir(obj)
    assert "new_field" in result


def test_dir_singleton(db0_fixture):
    obj = MemoSingleton(7)
    result = dir(obj)
    assert "value" in result
    assert "count" in result


def test_dir_returns_list(db0_fixture):
    obj = MemoSimple(1, "test")
    assert isinstance(dir(obj), list)


def test_dir_result_is_sorted(db0_fixture):
    obj = MemoSimple(1, "test")
    result = dir(obj)
    assert result == sorted(result)


# --- vars() tests ---

def test_vars_returns_dict(db0_fixture):
    obj = MemoSimple(42, "hello")
    result = vars(obj)
    assert isinstance(result, dict)


def test_vars_contains_init_fields(db0_fixture):
    obj = MemoSimple(42, "hello")
    result = vars(obj)
    assert "value" in result
    assert "name" in result
    assert result["value"] == 42
    assert result["name"] == "hello"


def test_vars_values_match_getattr(db0_fixture):
    obj = MemoSimple(99, "world")
    result = vars(obj)
    for key, val in result.items():
        assert getattr(obj, key) == val


def test_vars_excludes_deleted_field(db0_fixture):
    obj = MemoWithDeleted(5)
    result = vars(obj)
    assert "value" in result
    assert "temp" not in result


def test_vars_excludes_field_deleted_after_init(db0_fixture):
    obj = MemoSimple(1, "test")
    del obj.name
    result = vars(obj)
    assert "value" in result
    assert "name" not in result


def test_vars_for_conditional_field_present(db0_fixture):
    obj = MemoConditional(10, include_extra=True)
    result = vars(obj)
    assert result["value"] == 10
    assert result["extra"] == "extra"


def test_vars_for_conditional_field_absent(db0_fixture):
    obj = MemoConditional(10, include_extra=False)
    result = vars(obj)
    assert "value" in result
    assert "extra" not in result


def test_vars_dynamic_attrs(db0_fixture):
    obj = MemoDynamic(a=1, b="two", c=3.0)
    result = vars(obj)
    assert result["a"] == 1
    assert result["b"] == "two"
    assert result["c"] == 3.0


def test_vars_after_update(db0_fixture):
    obj = MemoSimple(1, "old")
    obj.name = "new"
    result = vars(obj)
    assert result["name"] == "new"


def test_vars_after_new_attr_assigned(db0_fixture):
    obj = MemoSimple(1, "a")
    obj.extra = 42
    result = vars(obj)
    assert "extra" in result
    assert result["extra"] == 42


def test_vars_singleton(db0_fixture):
    obj = MemoSingleton(7)
    result = vars(obj)
    assert result["value"] == 7
    assert "count" in result


def test_vars_does_not_include_methods(db0_fixture):
    obj = MemoWithMethod(10)
    result = vars(obj)
    assert "get_value" not in result
    assert "double" not in result


# --- __dict__ tests ---

def test_dict_returns_dict(db0_fixture):
    obj = MemoSimple(42, "hello")
    assert isinstance(obj.__dict__, dict)


def test_dict_contains_init_fields(db0_fixture):
    obj = MemoSimple(42, "hello")
    assert "value" in obj.__dict__
    assert "name" in obj.__dict__
    assert obj.__dict__["value"] == 42
    assert obj.__dict__["name"] == "hello"


def test_dict_equals_vars(db0_fixture):
    obj = MemoSimple(42, "hello")
    assert obj.__dict__ == vars(obj)


def test_dict_excludes_deleted_field(db0_fixture):
    obj = MemoWithDeleted(5)
    assert "value" in obj.__dict__
    assert "temp" not in obj.__dict__


def test_dict_excludes_field_deleted_after_init(db0_fixture):
    obj = MemoSimple(1, "test")
    del obj.name
    assert "value" in obj.__dict__
    assert "name" not in obj.__dict__


def test_dict_for_conditional_field_present(db0_fixture):
    obj = MemoConditional(10, include_extra=True)
    assert obj.__dict__["value"] == 10
    assert obj.__dict__["extra"] == "extra"


def test_dict_for_conditional_field_absent(db0_fixture):
    obj = MemoConditional(10, include_extra=False)
    assert "value" in obj.__dict__
    assert "extra" not in obj.__dict__


def test_dict_dynamic_attrs(db0_fixture):
    obj = MemoDynamic(a=1, b="two", c=3.0)
    assert obj.__dict__["a"] == 1
    assert obj.__dict__["b"] == "two"
    assert obj.__dict__["c"] == 3.0


def test_dict_reflects_update(db0_fixture):
    obj = MemoSimple(1, "old")
    obj.name = "new"
    assert obj.__dict__["name"] == "new"


def test_dict_after_new_attr_assigned(db0_fixture):
    obj = MemoSimple(1, "a")
    obj.extra = 42
    assert "extra" in obj.__dict__
    assert obj.__dict__["extra"] == 42


def test_dict_does_not_include_methods(db0_fixture):
    obj = MemoWithMethod(10)
    assert "get_value" not in obj.__dict__
    assert "double" not in obj.__dict__


def test_dict_singleton(db0_fixture):
    obj = MemoSingleton(7)
    assert obj.__dict__["value"] == 7
    assert "count" in obj.__dict__


def test_dict_values_match_getattr(db0_fixture):
    obj = MemoSimple(99, "world")
    for key, val in obj.__dict__.items():
        assert getattr(obj, key) == val


# --- edge cases ---

def test_dir_empty_memo(db0_fixture):
    obj = MemoEmpty()
    result = dir(obj)
    assert isinstance(result, list)
    assert result == sorted(result)


def test_vars_empty_memo(db0_fixture):
    obj = MemoEmpty()
    assert vars(obj) == {}


def test_dict_empty_memo(db0_fixture):
    obj = MemoEmpty()
    assert obj.__dict__ == {}


def test_dir_inherited_fields(db0_fixture):
    obj = MemoDerived(10, 20)
    result = dir(obj)
    assert "base_val" in result
    assert "derived_val" in result


def test_vars_inherited_fields(db0_fixture):
    obj = MemoDerived(10, 20)
    result = vars(obj)
    assert result["base_val"] == 10
    assert result["derived_val"] == 20


def test_dict_inherited_fields(db0_fixture):
    obj = MemoDerived(10, 20)
    assert obj.__dict__["base_val"] == 10
    assert obj.__dict__["derived_val"] == 20


def test_vars_none_values_included(db0_fixture):
    obj = MemoWithNone()
    result = vars(obj)
    assert "a" in result
    assert result["a"] is None
    assert result["b"] == 0
    assert result["c"] == ""


def test_dict_none_values_included(db0_fixture):
    obj = MemoWithNone()
    assert obj.__dict__["a"] is None
    assert obj.__dict__["b"] == 0
    assert obj.__dict__["c"] == ""


def test_dir_none_values_included(db0_fixture):
    obj = MemoWithNone()
    result = dir(obj)
    assert "a" in result
    assert "b" in result
    assert "c" in result


def test_vars_complex_field_types(db0_fixture):
    nested = MemoSimple(1, "nested")
    obj = MemoWithComplexFields([1, 2, 3], {"k": "v"}, nested)
    result = vars(obj)
    assert result["items"] == [1, 2, 3]
    assert result["mapping"] == {"k": "v"}
    assert result["nested"] is nested


def test_dict_complex_field_types(db0_fixture):
    nested = MemoSimple(1, "nested")
    obj = MemoWithComplexFields([1, 2, 3], {"k": "v"}, nested)
    assert obj.__dict__["items"] == [1, 2, 3]
    assert obj.__dict__["mapping"] == {"k": "v"}
    assert obj.__dict__["nested"] is nested


def test_dir_excludes_non_persistent_attrs(db0_fixture):
    obj = MemoWithNonPersistent(42)
    result = dir(obj)
    assert "value" in result
    assert "_X__cache" not in result


def test_vars_excludes_non_persistent_attrs(db0_fixture):
    obj = MemoWithNonPersistent(42)
    result = vars(obj)
    assert "value" in result
    assert "_X__cache" not in result


def test_dict_excludes_non_persistent_attrs(db0_fixture):
    obj = MemoWithNonPersistent(42)
    assert "value" in obj.__dict__
    assert "_X__cache" not in obj.__dict__


def test_vars_keys_are_subset_of_dir(db0_fixture):
    obj = MemoSimple(1, "test")
    assert set(vars(obj).keys()) <= set(dir(obj))


def test_dict_keys_are_subset_of_dir(db0_fixture):
    obj = MemoWithMethod(10)
    assert set(obj.__dict__.keys()) <= set(dir(obj))


def test_dir_property_not_in_vars(db0_fixture):
    obj = MemoWithProperty(5)
    # 'value' is a property on the type, not a stored persistent field
    assert "value" in dir(obj)
    assert "value" not in vars(obj)


def test_vars_after_fetch(db0_fixture):
    obj = MemoSimple(77, "fetched")
    fetched = db0.fetch(db0.uuid(obj))
    assert vars(fetched)["value"] == 77
    assert vars(fetched)["name"] == "fetched"


def test_dict_after_fetch(db0_fixture):
    obj = MemoSimple(77, "fetched")
    fetched = db0.fetch(db0.uuid(obj))
    assert fetched.__dict__["value"] == 77
    assert fetched.__dict__["name"] == "fetched"


def test_dir_after_fetch(db0_fixture):
    obj = MemoSimple(77, "fetched")
    fetched = db0.fetch(db0.uuid(obj))
    result = dir(fetched)
    assert "value" in result
    assert "name" in result


def test_vars_many_dynamic_fields(db0_fixture):
    kwargs = {f"field_{i}": i for i in range(20)}
    obj = MemoDynamic(**kwargs)
    result = vars(obj)
    for key, val in kwargs.items():
        assert result[key] == val


def test_dir_many_dynamic_fields(db0_fixture):
    kwargs = {f"field_{i}": i for i in range(20)}
    obj = MemoDynamic(**kwargs)
    result = dir(obj)
    for key in kwargs:
        assert key in result


def test_vars_immutable_memo(db0_fixture):
    obj = MemoImmutable(data="hello", count=3)
    result = vars(obj)
    assert result["data"] == "hello"
    assert result["count"] == 3


def test_dict_immutable_memo(db0_fixture):
    obj = MemoImmutable(data="hello", count=3)
    assert obj.__dict__["data"] == "hello"
    assert obj.__dict__["count"] == 3


def test_dir_immutable_memo(db0_fixture):
    obj = MemoImmutable(data="hello", count=3)
    result = dir(obj)
    assert "data" in result
    assert "count" in result


def test_vars_is_snapshot_not_live_view(db0_fixture):
    obj = MemoSimple(1, "original")
    snapshot = vars(obj)
    obj.name = "modified"
    # snapshot taken before modification should be unaffected
    assert snapshot["name"] == "original"
    # fresh call reflects update
    assert vars(obj)["name"] == "modified"


def test_dict_is_snapshot_not_live_view(db0_fixture):
    obj = MemoSimple(1, "original")
    snapshot = obj.__dict__
    obj.name = "modified"
    assert snapshot["name"] == "original"
    assert obj.__dict__["name"] == "modified"


# --- persistence tests ---

def test_vars_after_reopen(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoSimple(42, "persistent")
    obj_uuid = db0.uuid(obj)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = db0.fetch(obj_uuid)
    result = vars(reloaded)
    assert result["value"] == 42
    assert result["name"] == "persistent"


def test_dict_after_reopen(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoSimple(42, "persistent")
    obj_uuid = db0.uuid(obj)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = db0.fetch(obj_uuid)
    assert reloaded.__dict__["value"] == 42
    assert reloaded.__dict__["name"] == "persistent"


def test_dir_after_reopen(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoSimple(42, "persistent")
    obj_uuid = db0.uuid(obj)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = db0.fetch(obj_uuid)
    result = dir(reloaded)
    assert "value" in result
    assert "name" in result


def test_vars_after_reopen_with_deleted_field(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoSimple(1, "temp")
    del obj.name
    obj_uuid = db0.uuid(obj)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = db0.fetch(obj_uuid)
    result = vars(reloaded)
    assert "value" in result
    assert "name" not in result


def test_vars_after_reopen_conditional_field_present(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoConditional(10, include_extra=True)
    obj_uuid = db0.uuid(obj)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = db0.fetch(obj_uuid)
    result = vars(reloaded)
    assert result["value"] == 10
    assert result["extra"] == "extra"


def test_vars_after_reopen_conditional_field_absent(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoConditional(10, include_extra=False)
    obj_uuid = db0.uuid(obj)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = db0.fetch(obj_uuid)
    result = vars(reloaded)
    assert "value" in result
    assert "extra" not in result


def test_vars_after_reopen_reflects_update(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoSimple(1, "old")
    obj.name = "new"
    obj_uuid = db0.uuid(obj)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = db0.fetch(obj_uuid)
    assert vars(reloaded)["name"] == "new"


def test_vars_after_multiple_commits(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoSimple(1, "v1")
    obj_uuid = db0.uuid(obj)
    db0.commit()
    obj.name = "v2"
    db0.commit()
    obj.value = 99
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = db0.fetch(obj_uuid)
    result = vars(reloaded)
    assert result["value"] == 99
    assert result["name"] == "v2"


def test_vars_singleton_after_reopen(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoSingleton(55)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = MemoSingleton()
    result = vars(reloaded)
    assert result["value"] == 55
    assert "count" in result


def test_vars_after_reopen_dynamic_fields(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    obj = MemoDynamic(alpha=1, beta="two", gamma=3.0)
    obj_uuid = db0.uuid(obj)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    reloaded = db0.fetch(obj_uuid)
    result = vars(reloaded)
    assert result["alpha"] == 1
    assert result["beta"] == "two"
    assert result["gamma"] == 3.0
