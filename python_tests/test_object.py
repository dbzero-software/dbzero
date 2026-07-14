# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, DynamicDataClass, MemoTestSingleton


@db0.memo()
class DataClass:
    def __init__(self):
        pass
    
    def get_int_constant(self) -> int:
        return 1
    

@db0.memo()
class DataClassReadFromInit:
    def __init__(self, value: int, result: list):
        self.value = value
        # read value on init (from object's initializer buffer)
        result.append(self.value)


@db0.memo()
class DataClassWithAttr:
    def __init__(self, value: int):
        self.value = value
    
    def get_value(self) -> int:        
        return self.value


@db0.memo()
class DataClassMultiAttr:
    def __init__(self, value1, value2):
        self.value_1 = value1
        self.value_2 = value2
    
    def get_values(self):
        return self.value_1, self.value_2


@db0.memo
class DataClassAttrMutation:
    def __init__(self, value):
        self.value = 123
        self.value = value


@db0.memo
class MemoWithSelfRef:
    def __init__(self):
        self.value = self


@db0.memo
class DataClassWithRepr:
    def __init__(self, value):
        self.value = value
    
    def __repr__(self):
        return f"<DataClassWithRepr value={self.value}>"


@db0.memo
class DataClassWithComparators:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        if isinstance(other, DataClassWithComparators):
            return self.value == other.value
        return NotImplemented
    
    def __ne__(self, other: object) -> bool:
        if isinstance(other, DataClassWithComparators):
            return self.value != other.value
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, DataClassWithComparators):
            return self.value < other.value
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, DataClassWithComparators):
            return self.value <= other.value
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, DataClassWithComparators):
            return self.value > other.value
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, DataClassWithComparators):
            return self.value >= other.value
        return NotImplemented

@db0.memo
class DataClassWithMinimalComparators:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        if isinstance(other, DataClassWithMinimalComparators):
            return self.value == other.value
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, DataClassWithMinimalComparators):
            return self.value < other.value
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, DataClassWithMinimalComparators):
            return self.value <= other.value
        return NotImplemented


@db0.memo
class MemoBase():
    def __init__(self, value):
        self.value = value

@db0.memo
class MemoChild(MemoBase):
    def __init__(self, value, value_child):
        super().__init__(value)
        self.value_child = value_child


def test_create_memo_object(db0_fixture):
    object_1 = DataClass()
    assert object_1 is not None


def test_call_memo_object(db0_fixture):
    object_1 = DataClass()
    assert object_1.get_int_constant() == 1


def test_memo_object_access_member_from_init(db0_fixture):
    result = []
    object_1 = DataClassReadFromInit(957, result)
    assert result[0] == 957


def test_memo_object_setget_int_attr(db0_fixture):
    object_1 = DataClassWithAttr(123)
    assert object_1.get_value() == 123


def test_db0_object_multiple_allocs(db0_fixture):
    cache_0 = db0.get_cache_stats()
    objects = [DataClassWithAttr(123) for _ in range(256)]
    cache_1 = db0.get_cache_stats()
    # make sure cache utlization increased after object instance has been created
    assert cache_1["size"] > cache_0["size"]
    objects = []


def test_db0_object_gets_unlocked_when_memo_object_deleted(db0_fixture):    
    # collect initial cache statistics    
    db0.clear_cache()
    cache_0 = db0.get_cache_stats()
    # create multiple objects, so that mutliple pages are affected
    objects = [DataClassWithAttr(123) for _ in range(96)]
    # collect cache statistics while object is locked
    cache_1 = db0.get_cache_stats()    
    # make sure cache utilization increased after object instance has been created    
    assert cache_1["size"] > cache_0["size"]
    # delete memo objects
    for obj in objects:
        db0.delete(obj)
    del objects    
    db0.clear_cache()
    cache_2 = db0.get_cache_stats()
    # after deletion, some cache should be freed    
    assert cache_2["size"] < cache_1["size"]


def test_memo_object_setget_long_str_attr(db0_fixture):
    object_1 = DataClassWithAttr("abcdefghijklmnopqrstuvwxyz")
    assert object_1.get_value() == "abcdefghijklmnopqrstuvwxyz"


def test_memo_object_setget_object_attr(db0_fixture):
    object_1 = DataClassWithAttr(123)
    object_2 = DataClassWithAttr(object_1)
    assert object_2.get_value().get_value() == 123


def test_memo_object_multiple_attributes(db0_fixture):
    object_1 = DataClassMultiAttr(123, "value X")
    assert object_1.get_values() == (123, "value X")


def test_memo_object_attribute_update_during_init(db0_fixture):
    object_1 = DataClassAttrMutation(999)
    assert object_1.value == 999


def test_memo_object_attribute_update_after_init(db0_fixture):
    object_1 = DataClassWithAttr(999)
    object_1.value = 123    
    assert object_1.value == 123    


def test_memo_object_attribute_change_type(db0_fixture):
    object_1 = DataClassMultiAttr(123, "value X")
    object_1.value_1 = "value Y"
    object_1.value_2 = 123
    assert object_1.get_values() == ("value Y", 123)


def test_memo_object_can_be_deleted(db0_fixture):
    object_1 = DataClassWithAttr(123)
    db0.delete(object_1)
    # make sure subsequence access attempt will raise an exception
    with pytest.raises(Exception):
        object_1.get_value()


def test_attempt_using_object_after_prefix_close(db0_fixture):
    object_1 = DataClassWithAttr(123)
    db0.close()
    # attempt to update object should raise an exception    
    with pytest.raises(Exception):
        object_1.value = 456


def test_memo_object_can_keep_reference_to_self(db0_fixture):
    object_1 = MemoWithSelfRef()
    assert db0.uuid(object_1.value) == db0.uuid(object_1)


def test_memo_object_equality(db0_fixture):
    object_1 = DataClassMultiAttr(123, "value X")
    object_2 = DataClassMultiAttr(781, object_1)
    db0.commit()
    object3 =  db0.fetch(db0.uuid(object_1))
    assert object_1 == object3
    assert object_2.value_2 == object_1
    assert object_1 != object_2
    
    
def test_memo_object_destroys_its_pos_vt_dependencies(db0_fixture):
    obj = MemoTestClass(MemoTestClass(123))
    dep_uuid = db0.uuid(obj.value)
    # make sure member is stored as pos-vt
    # NOTE: there might be more slots preallocated for lo-fi members
    assert len(db0.describe(obj)["field_layout"]["pos_vt"]) >= 1
    db0.delete(obj)
    del obj    
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)
        
    
def test_memo_object_destroys_its_index_vt_dependencies(db0_fixture):
    _ = DynamicDataClass(120)
    obj = DynamicDataClass([0, 1, 2, 11, 33, 119], values = {0:0, 1:1, 2:2, 11:"a", 33:"b", 119:MemoTestClass(119)})
    dep_uuid = db0.uuid(obj.field_119)
    # make sure member is stored as index-vt    
    assert len(db0.describe(obj)["field_layout"]["index_vt"]) == 3
    db0.delete(obj)
    del obj    
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)
        
    
def test_memo_object_destroys_its_kv_dependencies(db0_fixture):
    obj = DynamicDataClass(5)
    obj.field_60 = MemoTestClass(60)
    dep_uuid = db0.uuid(obj.field_60)
    # make sure member is stored as index-vt    
    assert len(db0.describe(obj)["field_layout"]["kv_index"]) == 1
    db0.delete(obj)
    del obj    
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)


def test_memo_object_destroys_its_pos_vt_members_on_reassign(db0_fixture):
    obj = MemoTestClass(MemoTestClass(123))
    dep_uuid = db0.uuid(obj.value)
    # member should be dropped
    obj.value = None    
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)


def test_memo_object_destroys_its_index_vt_members_on_reassign(db0_fixture):
    obj_1 = DynamicDataClass(120)
    obj = DynamicDataClass([0, 1, 2, 11, 33, 119], values = {0:0, 1:1, 2:2, 11:None, 33:None, 119:MemoTestClass(119)})
    dep_uuid = db0.uuid(obj.field_119)
    # member should be dropped
    obj.field_119 = None    
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)


def test_memo_object_destroys_its_kv_members_on_reassign(db0_fixture):
    obj = DynamicDataClass(5)
    # assign None first (to check unreferencing None values on reassign)
    obj.field_60 = None
    # assign memo object next
    obj.field_60 = MemoTestClass(60)
    dep_uuid = db0.uuid(obj.field_60)
    # member should be dropped
    obj.field_60 = None    
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)


def test_repr_should_not_be_overriden(db0_fixture):
    obj = DataClassWithRepr(1)
    assert "<DataClassWithRepr value=1>" in repr(obj)


def test_comparators_should_work_for_memo_object_if_defined(db0_fixture):
    obj1 = DataClassWithComparators(5)
    obj2 = DataClassWithComparators(10)

    # Test __eq__ and __ne__
    assert obj1 == obj1
    assert obj1 != obj2

    # Test __lt__
    assert obj1 < obj2
    assert not obj2 < obj1

    # Test __le__
    assert obj1 <= obj1
    assert obj1 <= obj2
    assert not obj2 <= obj1

    # Test __gt__
    assert obj2 > obj1
    assert not obj1 > obj2

    # Test __ge__
    assert obj2 >= obj2
    assert obj2 >= obj1
    assert not obj1 >= obj2


def test_memo_object_ref_counting(db0_fixture):
    object_2 = MemoTestClass(0)
    object_1 = MemoTestClass(object_2)
    # object_2 is referenced by object_1
    assert db0.getrefcount(object_2) == 1


def test_memo_classes_ref_counting(db0_fixture):
    count_1 = db0.getrefcount(MemoTestClass)
    # each new instance should increase ref-count
    object_2 = MemoTestClass(0)
    count_2 = db0.getrefcount(MemoTestClass)
    object_1 = MemoTestClass(object_2)
    count_3 = db0.getrefcount(MemoTestClass)
    # may be more references when first object is created
    assert count_2 >= count_1 + 1
    assert count_3 == count_2 + 1
    

def test_comparators_should_work_for_memo_when_opposed_opperator_is_defined(db0_fixture):
    obj1 = DataClassWithMinimalComparators(5)
    obj2 = DataClassWithMinimalComparators(10)

    # Test __eq__
    assert obj1 == obj1
    # Test __ne__ as opposed to __eq__
    assert obj1 != obj2

    # Test __lt__
    assert obj1 < obj2
    assert not obj2 < obj1

    # Test __le__
    assert obj1 <= obj1
    assert obj1 <= obj2
    assert not obj2 <= obj1

    # Test __gt__ 
    assert obj2 > obj1
    assert not obj1 > obj2

    # Test __ge__
    assert obj2 >= obj2
    assert obj2 >= obj1
    assert not obj1 >= obj2


def test_equality_operator_on_instances_created_in_line(db0_fixture):
    # Test __eq__ and __ne__
    assert DataClassWithComparators(10) == DataClassWithComparators(10)
    assert DataClassWithComparators(5) != DataClassWithComparators(10)

    # Test __lt__
    assert DataClassWithComparators(5) < DataClassWithComparators(10)
    assert not DataClassWithComparators(10) < DataClassWithComparators(5)

    # Test __le__
    assert DataClassWithComparators(5) <= DataClassWithComparators(5)
    assert DataClassWithComparators(5) <= DataClassWithComparators(10)
    assert not DataClassWithComparators(10) <= DataClassWithComparators(5)

    # Test __gt__
    assert DataClassWithComparators(10) > DataClassWithComparators(5)
    assert not DataClassWithComparators(5) > DataClassWithComparators(10)

    # Test __ge__
    assert DataClassWithComparators(10) >= DataClassWithComparators(10)
    assert DataClassWithComparators(10) >= DataClassWithComparators(5)
    assert not DataClassWithComparators(5) >= DataClassWithComparators(10)

@db0.memo
class DataClassWitEQComparators:
    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        if isinstance(other, DataClassWitEQComparators):
            return self.value == other.value
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, DataClassWitEQComparators):
            return self.value < other.value
        return NotImplemented


def test_compare_works_with_not_implemented_operator(db0_fixture):
    # Test __eq__ and __ne__
    assert DataClassWitEQComparators(10) == DataClassWitEQComparators(10)
    assert DataClassWitEQComparators(5) != DataClassWitEQComparators(10)

    # Test __lt__
    assert DataClassWitEQComparators(5) < DataClassWitEQComparators(10)

    assert not DataClassWitEQComparators(5) > DataClassWitEQComparators(10)


def test_hasattr_on_memo_objects(db0_fixture):
    obj = MemoTestClass(0)
    assert hasattr(obj, "value")
    assert not hasattr(obj, "__ft__")
    
    
def test_object_fetch_as_memo_base(db0_fixture):
    obj_1 = MemoTestClass(123)
    _ = MemoTestSingleton(obj_1)
    uuid_1 = db0.uuid(obj_1)
    del obj_1
    db0.clear_cache()
    db0.commit()
    obj_2 = db0.fetch(uuid_1, db0.MemoBase)
    assert obj_2.value == 123
    

def test_child_object_comparison(db0_fixture):
    child = MemoChild(123, 456)
    obj_1 = MemoTestClass(child)
    assert obj_1.value == child
    
    
def test_memo_object_ref_counting_with_tags(db0_fixture):
    object_2 = MemoTestClass(0)
    db0.tags(object_2).add("test-1")
    assert db0.getrefcount(object_2) == 1
    object_1 = MemoTestClass(object_2)
    assert db0.getrefcount(object_2) == 2
    db0.tags(object_2).add("test-2")
    assert db0.getrefcount(object_2) == 3
    db0.tags(object_2).remove("test-1", "test-2")
    assert db0.getrefcount(object_2) == 1
