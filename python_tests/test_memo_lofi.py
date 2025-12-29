# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestThreeParamsClass, MemoAnyAttrs


def test_memo_store_single_bool(db0_fixture):
    obj_1 = MemoTestClass(True)
    assert db0.describe(obj_1)["field_layout"]["pos_vt"] == ["PACK_2"]


def test_memo_store_multiple_bools(db0_fixture):
    obj_1 = MemoTestClass(True)
    obj_2 = MemoTestThreeParamsClass(True, False, True)
    assert db0.describe(obj_2)["size_of"] == db0.describe(obj_1)["size_of"]


def test_memo_read_bool(db0_fixture):
    obj_1 = MemoTestThreeParamsClass(True, False, True)
    assert obj_1.value_1 is True
    assert obj_1.value_2 is False
    assert obj_1.value_3 is True


def test_memo_store_multiple_nones(db0_fixture):
    obj_1 = MemoTestClass(True)
    obj_2 = MemoTestThreeParamsClass(None, None, None)
    assert db0.describe(obj_2)["size_of"] == db0.describe(obj_1)["size_of"]


def test_memo_mixed_bool_none_storage(db0_fixture):
    obj_1 = MemoTestClass(True)
    obj_2 = MemoTestThreeParamsClass(True, None, False)
    assert db0.describe(obj_2)["size_of"] == db0.describe(obj_1)["size_of"]
    assert obj_2.value_1 is True
    assert obj_2.value_2 is None
    assert obj_2.value_3 is False


def test_memo_lofi_attr_availability(db0_fixture):
    obj_1 = MemoAnyAttrs(value1=True, value2=None, value3=False)
    obj_2 = MemoAnyAttrs(value1=True)
    assert hasattr(obj_1, "value1")
    assert hasattr(obj_1, "value2")
    assert hasattr(obj_1, "value3")
    assert hasattr(obj_2, "value1")
    assert not hasattr(obj_2, "value2")
    assert not hasattr(obj_2, "value3")


def test_memo_lofi_update_with_same_fidelity(db0_fixture):
    obj_1 = MemoAnyAttrs(value1=True, value2=None, value3=False)    
    obj_1.value2 = True
    obj_1.value3 = None
    assert obj_1.value1 is True
    assert obj_1.value2 is True
    assert obj_1.value3 is None
    # make sure layout not changed
    assert db0.describe(obj_1)["field_layout"]["pos_vt"] == ["PACK_2"]
    assert not db0.describe(obj_1)["field_layout"]["index_vt"]
    assert not db0.describe(obj_1)["field_layout"]["kv_index"]
    
    
def test_memo_overwrite_lofi_with_full_value(db0_fixture):
    obj_1 = MemoAnyAttrs(value1=True, value2=None, value3=False)
    obj_1.value1 = "Full length value"
    assert obj_1.value1 == "Full length value"
    assert obj_1.value2 is None
    assert obj_1.value3 is False
    assert len(db0.describe(obj_1)["field_layout"]["kv_index"]) > 0
    
    
def test_memo_overwrite_full_value_with_lofi(db0_fixture):
    obj_1 = MemoAnyAttrs(value1=True, value2="Full length value", value3=False)
    obj_1.value2 = False
    assert obj_1.value1 is True
    assert obj_1.value2 is False
    assert obj_1.value3 is False
    

def test_memo_extend_by_lofi_value(db0_fixture):
    obj_1 = MemoAnyAttrs(value1=True, value2="Full length value", value3=False)
    size_before = db0.describe(obj_1)["size_of"]
    obj_1.new_value = True    
    assert obj_1.new_value is True
    assert db0.describe(obj_1)["size_of"] == size_before
    
    
def test_memo_store_lofi_values_in_kv_index(db0_fixture):
    obj_1 = MemoAnyAttrs(value1=True, value2="Full length value", value3=False)
    # Add lo-fi values exceeding capacity of the existing slots
    for i in range(100):
        setattr(obj_1, f"new_value_{i}", True)
    
    # First attribute outside of the shared lo-fi slots
    assert getattr(obj_1, "new_value_19") is True
    for i in range(100):        
        assert getattr(obj_1, f"new_value_{i}") is True        
    
    # make sure kv-index values are packed
    assert len(db0.describe(obj_1)["field_layout"]["kv_index"]) < 10
    

def test_update_lofi_values_in_kv_index(db0_fixture):
    obj_1 = MemoAnyAttrs(value1=True, value2="Full length value", value3=False)    
    for i in range(100):
        setattr(obj_1, f"new_value_{i}", True)
    size_before = db0.describe(obj_1)["size_of"]
    # Update with other lo-fi values
    setattr(obj_1, "new_value_50", False)
    setattr(obj_1, "new_value_82", None)
    setattr(obj_1, "new_value_33", False)
    assert getattr(obj_1, "new_value_50") is False
    assert getattr(obj_1, "new_value_82") is None
    assert getattr(obj_1, "new_value_33") is False
    assert db0.describe(obj_1)["size_of"] == size_before
    
    
def test_update_lofi_with_full_values_in_kv_index(db0_fixture):
    obj_1 = MemoAnyAttrs(value1=True, value2="Full length value", value3=False)    
    for i in range(100):
        setattr(obj_1, f"new_value_{i}", True)
    setattr(obj_1, "new_value_50", "Full length value")
    assert getattr(obj_1, "new_value_50") == "Full length value"
    assert getattr(obj_1, "new_value_49") ==  True
    assert getattr(obj_1, "new_value_51") ==  True
    assert "STRING_REF" in db0.describe(obj_1)["field_layout"]["kv_index"].values()
    