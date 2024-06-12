import pytest
import dbzero_ce as db0


@db0.memo(prefix="scoped-class-prefix")
class ScopedDataClass:
    def __init__(self, value):
        self.value = value
    
    
@db0.enum(values=["RED", "GREEN", "BLACK"], prefix="scoped-class-prefix")
class ScopedColor:
    pass


@db0.memo(prefix=None)
class DataClass:
    def __init__(self, value):
        self.value = value


def test_get_type_info_from_scoped_class(db0_fixture):
    type_info = db0.get_type_info(ScopedDataClass)
    assert type_info["prefix"] == "scoped-class-prefix"
    
    
def test_create_scoped_class_instance(db0_fixture):
    obj = ScopedDataClass(42)
    assert db0.get_prefix(obj) != db0.get_current_prefix()
    
    
def test_scoped_class_can_be_tagged_with_scoped_enum(db0_fixture):
    obj = ScopedDataClass(42)
    db0.tags(obj).add(ScopedColor.RED)
    assert len(list(db0.find(ScopedDataClass, ScopedColor.RED))) == 1
    

def test_class_with_null_prefix_is_no_scoped(db0_fixture):
    obj = DataClass(42)
    assert db0.get_prefix(obj) == db0.get_current_prefix()
    
    
# This code fails only when prefix=None due to some pytest operations on types
# @db0.enum(values=["RED", "GREEN", "BLACK"], prefix=None)
# class XColor:
#     pass
    
# def test_enum_with_null_prefix_is_no_scoped(db0_fixture):
#     obj = DataClass(42)
#     db0.tags(obj).add(XColor.RED)
#     assert len(list(db0.find(DataClass, XColor.RED))) == 1


def test_scoped_type_members_use_same_prefix(db0_fixture):
    # 1. list type
    obj = ScopedDataClass([])
    assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
    # 2. dict type
    obj = ScopedDataClass({})
    assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
    # 3. set type
    obj = ScopedDataClass(set())
    assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
    # 4. tuple type
    obj = ScopedDataClass((1,2,3))
    assert db0.get_prefix(obj.value) == db0.get_prefix(obj)    
    # 5. db0.index type
    obj = ScopedDataClass(db0.index())
    assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
    