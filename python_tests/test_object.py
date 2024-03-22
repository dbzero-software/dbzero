from typing import Any
import pytest
import dbzero_ce as db0
import pickle
import io


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


@db0.memo()
class DataClassAttrMutation:
    def __init__(self, value):
        self.value = 123
        self.value = value


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
    cache_0 = db0.cache_stats()
    objects = [DataClassWithAttr(123) for _ in range(256)]
    cache_1 = db0.cache_stats()
    # make sure cache utlization increased after object instance has been created
    assert cache_1["size"] > cache_0["size"]
    objects = []


def test_db0_object_gets_unlocked_when_memo_object_deleted(db0_fixture):
    # collect initial cache statistics    
    db0.clear_cache()
    cache_0 = db0.cache_stats()
    # create multiple objects, so that mutliple pages are affected
    objects = [DataClassWithAttr(123) for _ in range(96)]    
    db0.clear_cache()
    # collect cache statistics while object is locked
    cache_1 = db0.cache_stats()
    # make sure cache utilization increased after object instance has been created    
    assert cache_1["size"] > cache_0["size"]
    # delete memo objects
    del objects    
    db0.clear_cache()
    cache_2 = db0.cache_stats()
    # after deletion, some cache should be freed    
    assert cache_2["size"] < cache_1["size"]


def test_memo_object_setget_long_str_attr(db0_fixture):
    object_1 = DataClassWithAttr("abcdefghijklmnopqrstuvwxyz")
    assert object_1.get_value() == "abcdefghijklmnopqrstuvwxyz"


def test_memo_object_setget_object_attr(db0_fixture):
    object_1 = DataClassWithAttr(123)
    object_2 = DataClassWithAttr(object_1)
    assert object_2.get_value().get_value() == 123


def test_uid_of_memo_object(db0_fixture):
    object_1 = DataClassWithAttr(123) 
    assert db0.uuid(object_1) is not None


def test_uid_has_hex_repr(db0_fixture):
    object_1 = DataClassWithAttr(123)
    assert repr(db0.uuid(object_1)).startswith("0x")


def test_object_can_be_fetched_by_id(db0_fixture):
    object_1 = DataClassWithAttr(123)
    object_2 = db0.fetch(db0.uuid(object_1))
    assert object_2 is not None
    assert object_2.get_value() == 123


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


def test_objectid_can_be_pickled_and_unpickled(db0_fixture):
    object_1 = DataClassWithAttr(123)
    object_id = db0.uuid(object_1)
    
    pickled = pickle.dumps(object_id)    
    unpickled = pickle.loads(pickled)
    assert unpickled is not None
    assert object_id == unpickled
