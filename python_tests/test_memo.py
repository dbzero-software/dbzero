import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, TriColor


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
    Issue: https://github.com/wskozlowski/dbzero_ce/issues/237
    """    
    test_obj = MemoClassWithSetter(1)
    assert test_obj.value == 1
    test_obj.value = 2
    assert test_obj.value == 3
    del test_obj.value
    assert not hasattr(test_obj, "_value")