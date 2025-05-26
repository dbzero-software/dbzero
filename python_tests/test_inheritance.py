import pytest
import dbzero_ce as db0
from .conftest import DB0_DIR
from dbzero_ce import memo


@memo
class BaseClass:
    def __init__(self, base_value):
        self.base_value = base_value

    def base_method(self):
        return self.base_value        

@memo
class DerivedClass(BaseClass):
    def __init__(self, base_value, value):
        super().__init__(base_value)
        self.value = value


def test_derived_instance_is_created(db0_fixture):
    derived = DerivedClass(1, 2)
    assert derived.base_value == 1
    assert derived.value == 2
    id = db0.uuid(derived)
    prefix_name = db0.get_prefix_of(derived).name
    # close db0 and open as read-only
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")    
    object_1 = db0.fetch(id)
    assert object_1.base_value == 1
    assert object_1.value == 2


def test_call_method_from_base_class(db0_fixture):
    derived = DerivedClass(7, 2)
    assert derived.base_method() == 7
    
    
def test_child_class_eq_issue_1(db0_fixture):
    """
    Issue related with the following ticket:
    https://github.com/wskozlowski/dbzero_ce/issues/232
    """
    a = DerivedClass(1, 2)
    assert a is a
    assert a == a
