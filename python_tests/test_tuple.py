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
