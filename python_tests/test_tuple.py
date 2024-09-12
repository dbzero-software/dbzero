import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass

def test_can_create_tuple(db0_fixture):
    tuple_1 = db0.tuple([1, 2, 3, 4])
    assert len(tuple_1) == 4
    assert tuple_1[0] == 1
    assert tuple_1[2] == 3


def test_can_iterate_over_tuple(db0_fixture):
    tuple_1 = db0.tuple([1, 2, 3, 4])
    result = []
    for i in tuple_1:
        result.append(i)

    assert result == [1, 2, 3, 4]


def test_tuple_can_get_index(db0_fixture):
    tuple_1 = db0.tuple([1, 2, 3, 4])
    assert tuple_1.index(2) == 1


def test_tuple_can_get_count(db0_fixture):
    tuple_1 = db0.tuple([1, 2, 3, 2])
    assert tuple_1.count(1) == 1
    assert tuple_1.count(2) == 2
    
    
def test_tuples_can_be_unpacked(db0_fixture):
    tuple_1 = db0.tuple([1, 2, 3, 4])
    a, b, c, d = tuple_1
    assert a == 1
    assert b == 2
    assert c == 3
    assert d == 4
    
    
def test_tuples_can_store_bytes(db0_fixture):
    tuple_1 = db0.tuple([1, b"hello", "world"])
    a, b, c = tuple_1
    assert a == 1
    assert b == b"hello"
    assert c == "world"

def test_tuple_can_be_compared(db0_fixture):
    tuple_1 = db0.tuple([1, 2, 3, 4])
    tuple_2 = db0.tuple([1, 2, 3, 4])
    assert tuple_1 == tuple_2
    assert tuple_1 == (1, 2, 3, 4)

def test_tuples_destroy_removes_reference(db0_fixture):
    obj = MemoTestClass(db0.tuple([MemoTestClass("asd")]))
    assert obj.value[0] is not None

    dep_uuid = db0.uuid(obj.value[0])
    db0.delete(obj)
    del obj
    db0.clear_cache()
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)

def test_tuples_destroy_removes_reference_from_dict(db0_fixture):
    obj = MemoTestClass(db0.tuple([MemoTestClass({"a": "b"})]))
    assert obj.value[0] is not None

    dep_uuid = db0.uuid(obj.value[0])
    db0.delete(obj)
    del obj
    db0.clear_cache()
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)