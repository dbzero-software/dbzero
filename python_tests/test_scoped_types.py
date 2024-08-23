import pytest
import dbzero_ce as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoScopedClass, MemoTestClass


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


def test_hardening_of_non_empty_list_reference_not_supported(db0_fixture):
    with pytest.raises(Exception):
        obj = ScopedDataClass(db0.list([1,2,3]))


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


def test_auto_hardening_of_weak_index_references(db0_fixture):
    ix = db0.index()
    assert db0.get_prefix(ix) == db0.get_current_prefix()
    obj = ScopedDataClass(ix)
    obj.value.add(0, obj)
    # make sure object was moved to proper scope and the reference was hardened
    assert db0.get_prefix(obj.value) == db0.get_prefix(obj)
    db0.commit()


def dict_set(obj, values):
    obj[values[0]] = values[1]

def make_tuple():
    return db0.tuple([1,2,3])

dict_test_params = [(db0.index, lambda ix, values: ix.add(*values)),
                    (db0.dict, dict_set),
                    (db0.set, lambda set, values: set.add(values[1])),
                    (db0.list, lambda list, values: list.append(values[1])),
                    (make_tuple, lambda _, values: values[1])]


@pytest.mark.parametrize("make_add_param", dict_test_params)
def test_auto_hardening_of_weak_object_references(db0_fixture, make_add_param):
    make_obj, add_to_obj = make_add_param
    obj = make_obj()
    assert db0.get_prefix(obj) == db0.get_current_prefix()
    scoped_data = ScopedDataClass(obj)
    add_to_obj(scoped_data.value, (10, scoped_data))
    # make sure object was moved to proper scope and the reference was hardened
    assert db0.get_prefix(scoped_data.value) == db0.get_prefix(scoped_data)
    db0.commit()
    
    
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
    

def test_using_index_after_hardening(db0_fixture):
    obj = ScopedDataClass(db0.index())
    for i in range(10):
        obj.value.add(i, obj)    
    db0.commit()
    assert len(list(obj.value.range(0, 10))) == 10
    

@db0.memo(prefix="scoped-class-prefix")
class TestScopedContainer:
    def __init__(self):
        self.ix_test = db0.index()


@db0.memo(prefix="scoped-class-prefix")
class TestScopedData:
    def __init__(self, value):
        self.value = value


@db0.memo(prefix="scoped-class-prefix", singleton=True)
class TestScopedSingleton:
    def __init__(self):
        self.container = TestScopedContainer()

    
def test_zorch_scoped_types_issue(db0_fixture):
    """
    This test reproduces a problem first observed in Zorch
    """
    singleton = TestScopedSingleton()
    ix_test = singleton.container.ix_test
    for i in range(10):
        data = TestScopedData(i)
        ix_test.add(None, data)
    
    del singleton
    del ix_test
    
    ix_test = TestScopedSingleton().container.ix_test
    query = ix_test.range(None, 100, null_first=True)
    assert len(list(query)) == 10


def test_scoped_index_issue(db0_fixture):
    prefix = "test-data"
    obj = MemoScopedClass(db0.index(), prefix=prefix)    
    index = obj.value
    index.add(None, MemoScopedClass(100, prefix=prefix))
    assert len(list(index.range(None, 100, null_first=True))) == 1


def test_scoped_dict_issue(db0_fixture):
    prefix = "test-data"
    obj = MemoScopedClass(db0.dict(), prefix=prefix)    
    dict = obj.value
    dict["asd"] = MemoScopedClass(100, prefix=prefix)
    assert len(dict) == 1
    assert dict["asd"].value == 100


def test_scoped_set_issue(db0_fixture):
    prefix = "test-data"
    obj = MemoScopedClass(db0.set(), prefix=prefix)    
    set = obj.value
    set.add(MemoScopedClass(100, prefix=prefix))
    assert len(set) == 1


def test_scoped_list_issue(db0_fixture):
    prefix = "test-data"
    obj = MemoScopedClass(db0.list(), prefix=prefix)    
    list = obj.value
    list.append(MemoScopedClass(100, prefix=prefix))
    assert len(list) == 1
    assert list[0].value == 100
