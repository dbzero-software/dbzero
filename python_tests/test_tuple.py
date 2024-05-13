import pytest
import dbzero_ce as db0

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
