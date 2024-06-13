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
        

def test_class_with_null_prefix_is_no_scoped(db0_fixture):
    obj = DataClass(42)
    assert db0.get_prefix(obj) == db0.get_current_prefix()
    

def test_scoped_type_creation_does_not_change_current_prefix(db0_fixture):
    current_prefix = db0.get_current_prefix()    
    obj = ScopedDataClass([])
    assert db0.get_current_prefix() == current_prefix
    del obj


def test_list_as_a_scoped_type_member(db0_fixture):
    obj = ScopedDataClass([1,2,3])
    assert obj.value == [1,2,3]


# FIXME: test fails due to missing "reference hardening" feature
# def test_db0_list_as_a_scoped_type_member(db0_fixture):
#     obj = ScopedDataClass(db0.list([1,2,3]))
#     assert obj.value == [1,2,3]


def test_dict_as_a_scoped_type_member(db0_fixture):
    obj = ScopedDataClass({"a": 1, "b": 2})  
    assert dict(obj.value) == {"a": 1, "b": 2}
    

def test_set_as_a_scoped_type_member(db0_fixture):
    obj = ScopedDataClass(set([1,2,3]))
    assert set(obj.value) == set([1,2,3])


def test_tuple_as_a_scoped_type_member(db0_fixture):
    obj = ScopedDataClass((1,2,3))
    assert tuple(obj.value) == (1,2,3)


def test_tuple_as_a_scoped_type_member(db0_fixture):
    obj = ScopedDataClass((1,2,3))
    assert tuple(obj.value) == (1,2,3)


def test_auto_hardening_of_weak_references(db0_fixture):
    ix = db0.index()
    assert db0.get_prefix(ix) == db0.get_current_prefix()
    obj = ScopedDataClass(ix)
    obj.value.add(0, obj)
    # make sure object was moved to proper scope and the reference was hardened
    assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
    
    
@db0.memo(prefix="scoped-class-prefix", singleton=True)
class ScopedSingleton:
    def __init__(self, value):
        self.value = value


def test_scoped_singleton(db0_fixture):
    singleton = ScopedSingleton(42)
    assert db0.get_prefix(singleton) != db0.get_current_prefix()
    db0.commit()
    object = ScopedSingleton()
    assert object == singleton
    assert object.value == 42
    assert db0.get_prefix(object) == db0.get_prefix(singleton)
    

# def test_scoped_type_members_use_same_prefix(db0_fixture):
#     # 1. list type
#     obj = ScopedDataClass([])
#     assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
#     # 2. dict type
#     obj = ScopedDataClass({})
#     assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
#     # 3. set type
#     obj = ScopedDataClass(set())
#     assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
#     # 4. tuple type
#     obj = ScopedDataClass((1,2,3))
#     assert db0.get_prefix(obj.value) == db0.get_prefix(obj)    
#     # 5. db0.index type
#     x = db0.index()
#     obj = ScopedDataClass(x)
#     print(db0.get_prefix(x))
#     print(db0.get_prefix(obj.value))
#     assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
    