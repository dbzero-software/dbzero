# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, TriColor, MemoAnyAttrs
from dataclasses import dataclass


class RegularPyClass:
    pass

@db0.memo
class MemoTestEQClass:
    def __init__(self, value):
        self.value = value        
        
    def __eq__(self, value):
        if isinstance(value, MemoTestEQClass):
            return self.value == value.value


@db0.memo
class MemoClassWithSetter:
    def __init__(self, value):
        self._value = value
    
    @property
    def value(self):
        return self._value
    
    @value.setter
    def value(self, new_value):
        self._value = new_value + 1

    
def test_memo_is_instance_operator(db0_fixture):
    obj_1 = MemoTestClass(999)    
    obj_2 = db0.fetch(db0.uuid(obj_1))
    assert obj_1 is obj_2
    

def test_memo_default_eq_operator(db0_fixture):
    obj_1 = MemoTestClass(999)
    obj_2 = db0.fetch(db0.uuid(obj_1))
    # by default, the __eq__ operator fallbacks to the identity operator
    assert obj_1 == obj_2
    
    
def test_memo_overloaded_eq_operator(db0_fixture):
    obj_1 = MemoTestClass(999)
    obj_2 = MemoTestClass(999)
    obj_3 = MemoTestEQClass(999)
    obj_4 = MemoTestEQClass(999)
    assert obj_1 != obj_2
    assert obj_3 == obj_4
    assert obj_1 != obj_3
    
    
def test_is_memo(db0_fixture):
    assert db0.is_memo(MemoTestClass(1)) == True
    assert db0.is_memo(TriColor.RED) == False
    assert db0.is_memo(1) == False
    assert db0.is_memo("asd") == False
    assert db0.is_memo([1, 2, 3]) == False
    assert db0.is_memo({"a": 1, "b": 2}) == False
    assert db0.is_memo(db0.list([1,2,3])) == False
    
    
def test_is_memo_for_types(db0_fixture):
    assert db0.is_memo(MemoTestClass) == True
    assert db0.is_memo(RegularPyClass) == False
    
    
@pytest.mark.skip(reason="Skipping due to unresolved issue #237")
def test_memo_property_decorator_issue1(db0_fixture):
    """
    Issue: https://github.com/wskozlowski/dbzero/issues/237
    """    
    test_obj = MemoClassWithSetter(1)
    assert test_obj.value == 1
    test_obj.value = 2
    assert test_obj.value == 3
    del test_obj.value
    assert not hasattr(test_obj, "_value")

    
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 << 20}], indirect=True)
def test_memo_gc_issue1(db0_slab_size):
    """
    Issue: this test was causing a segfault on gc.collect() in Python 3.13, but not on earlier versions
    Resolution:
    """
    import gc
    from .data_for_tests import test_strings
    
    count = 0
    for _ in range(50000):
        str = test_strings[count % len(test_strings)]        
        obj = MemoTestClass(str)
        del obj
        count += 1        
    
    gc.collect()
    
    
def test_type_as_member(db0_fixture):
    with pytest.raises(Exception):
        # storing regular Python class not allowed
        obj_1 = MemoTestClass(RegularPyClass)
    
    obj_1 = MemoTestClass(MemoTestEQClass)
    assert obj_1.value is MemoTestEQClass
    
    
def test_unreference_type_member(db0_fixture):
    ref_cnt_1 = db0.getrefcount(MemoTestEQClass)
    obj_1 = MemoTestClass(MemoTestEQClass)
    ref_cnt_2 = db0.getrefcount(MemoTestEQClass)
    assert ref_cnt_2 > ref_cnt_1
    obj_1.value = None
    assert db0.getrefcount(MemoTestEQClass) < ref_cnt_2
    
    
def test_memo_field_deletion(db0_fixture):
    obj_1 = MemoTestClass(1)
    del obj_1.value
    assert not hasattr(obj_1, "value")
    
    
@db0.memo
class MemoClassWithDel:
    def __init__(self, value):
        self.value = value        
        # create value_2 and delete it before object is initialized
        self.value_2 = value + 1
        del self.value_2
    
    
def test_memo_field_deletion_in_pre_init(db0_fixture):
    obj_1 = MemoClassWithDel(1)
    with pytest.raises(AttributeError):
        assert obj_1.value_2 is None


def test_memo_type_as_memo_member(db0_fixture):
    obj_1 = MemoTestClass(MemoTestEQClass)
    obj_2 = MemoTestClass(MemoClassWithDel)
    assert obj_1.value is MemoTestEQClass
    assert obj_2.value is not MemoTestEQClass
    obj_x = obj_1.value(1)
    assert obj_x.value == 1
    assert type(obj_x) is MemoTestEQClass


@db0.memo(id="/Division By Zero/project/tests/MemoTypeIdClass")
class MemoTypeIdClass:
    def __init__(self):
        self.value = 123
    
    
def test_memo_class_with_typeid(db0_fixture):
    obj_1 = MemoTypeIdClass()
    assert obj_1.value == 123
    

@db0.memo
class MemoConditionalMember:
    def __init__(self, cond_value):
        if cond_value > 10:
            self.cond_value = None
            return
        self.cond_value = cond_value
    
    
def test_memo_unset_value_defaults_to_none(db0_fixture):
    obj_1 = MemoConditionalMember(15)
    assert obj_1.cond_value is None
    
    
def test_reassign_deleted_member(db0_fixture):
    obj_1 = MemoTestClass(1)
    del obj_1.value
    assert not hasattr(obj_1, "value")
    # assign after deletion
    obj_1.value = False
    assert obj_1.value == False
    # delete again
    del obj_1.value
    assert not hasattr(obj_1, "value")
    # assign with full-length value
    obj_1.value = "Full Length Value"
    assert obj_1.value == "Full Length Value"
    
    
def test_selective_assign_members(db0_fixture):
    obj_1 = MemoAnyAttrs(f1 = 0, f2 = 1, f3 = 2)
    # NOTE: additional slot assigned to pack-2 values (initially unused)
    assert len(db0.describe(obj_1)["field_layout"]["pos_vt"]) == 4
    # assigned at first use
    obj_1.f2 = False
    assert len(db0.describe(obj_1)["field_layout"]["pos_vt"]) == 4
    
    _ = MemoAnyAttrs(f4 = False, f5 = 1, f6 = 2, f7 = 3.5, f8 = 1, f9 = 2)
    obj_3 = MemoAnyAttrs(f7 = 3.5, f8 = 1, f9 = 2)
    assert len(db0.describe(obj_3)["field_layout"]["pos_vt"]) == 3
    # NOTE: pack-2 slot included on condition fill-rate is at least 50%
    obj_4 = MemoAnyAttrs(f4 = False, f5 = 1, f6 = 2, f7 = 3.5)
    assert len(db0.describe(obj_4)["field_layout"]["pos_vt"]) >= 4    
    obj_5 = MemoAnyAttrs(f4 = False, f6 = 1, f9 = 11)
    # too spread apart, only some fraction of slots to be allocated to pos-vt
    assert len(db0.describe(obj_5)["field_layout"]["pos_vt"]) < 3
    