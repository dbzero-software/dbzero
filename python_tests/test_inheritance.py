# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .conftest import DB0_DIR
from dbzero import memo


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


class HasColor:
    def __init__(self, color):
        self.color = color

@memo
class MemoWithColor(HasColor):
    def __init__(self, amount, color):        
        self.amount = amount
        super().__init__(color)


@memo
class MultipleDerivedClass(DerivedClass, HasColor):
    def __init__(self, base_value, value, color):
        super().__init__(base_value, value)
        HasColor.__init__(self, color)
    
    
def test_derived_instance_is_created(db0_fixture):
    derived = DerivedClass(1, 2)
    # assign tag to persist the instance
    db0.tags(derived).add("temp")
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
    https://github.com/wskozlowski/dbzero/issues/232
    """
    a = DerivedClass(1, 2)
    assert a is a
    assert a == a


def test_memo_derived_from_regular_class(db0_fixture):
    obj = MemoWithColor(100, "red")
    assert obj.amount == 100
    assert obj.color == "red"


def test_destroying_derived_objects_when_untagged(db0_fixture):
    db0.tags(DerivedClass(0, 0)).add("tag-1")
    uuid = db0.uuid(next(iter(db0.find(DerivedClass, "tag-1"))))
    db0.commit()
    assert db0.exists(uuid)
    db0.tags(next(iter(db0.find(DerivedClass, "tag-1")))).remove("tag-1")
    db0.commit()
    assert not db0.exists(uuid)


def test_destroying_objects_with_multiple_bases_when_untagged(db0_fixture):
    db0.tags(MultipleDerivedClass(0, 0, color = "red")).add("tag-1")
    # NOTE: here we find by one of the bases
    uuid = db0.uuid(next(iter(db0.find(DerivedClass, "tag-1"))))
    db0.commit()
    assert db0.exists(uuid)
    db0.tags(next(iter(db0.find(DerivedClass, "tag-1")))).remove("tag-1")
    db0.commit()
    assert not db0.exists(uuid)
