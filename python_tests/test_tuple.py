# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass


# def test_can_create_empty_tuple(db0_fixture):
#     tuple_1 = db0.tuple()
#     assert len(tuple_1) == 0


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
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)


# FIXME: test failing with Type dbzero.Dict does not have a UUID (not implemented feature)
# def test_tuples_destroy_removes_reference_to_dict(db0_fixture):
#     obj = MemoTestClass(db0.tuple([{"a": "b"}]))
#     assert obj.value[0] is not None
#
#     dep_uuid = db0.uuid(obj.value[0])
#     db0.delete(obj)
#     del obj
#     db0.commit()
#     # make sure dependent instance has been destroyed as well
#     with pytest.raises(Exception):
#         db0.fetch(dep_uuid)


def test_tuple_from_generator(db0_fixture):
    t = db0.tuple((v for v in [1, 2, 'abc', 4, None]))
    assert t == (1, 2, 'abc', 4, None)


def test_empty_tuple(db0_fixture):
    t = db0.tuple()
    assert t == ()
    assert db0.tuple() == ()
    assert db0.tuple() == db0.tuple()


def test_invalid_constructor_arguments(db0_fixture):
    with pytest.raises(TypeError):
        db0.tuple(1,2)

    for arg in [1, 1.0, None, True, False]:
        with pytest.raises(TypeError):
            db0.tuple(arg)

    def broken_iterator():
        for i in range(100):
            yield i
        raise ValueError('TestError')
    
    for i in range(5):
        with pytest.raises(ValueError):
            db0.tuple(broken_iterator())


def test_db0_tuple_str_same_as_python_tuple(db0_fixture):
    db0_tuple = db0.tuple([1, "two", 3.0, None])
    py_tuple = (1, "two", 3.0, None)
    assert str(db0_tuple) == str(py_tuple)
    assert repr(db0_tuple) == repr(py_tuple)

def test_db0_tuple_str_with_nested_objects(db0_fixture):
    inner_tuple = db0.tuple([1, 2, 3])
    db0_tuple = db0.tuple([inner_tuple, "test", None])
    py_inner_tuple = (1, 2, 3)
    py_tuple = (py_inner_tuple, "test", None)
    assert str(db0_tuple) == str(py_tuple)
    assert repr(db0_tuple) == repr(py_tuple)

def test_db0_tuple_str_with_nested_memo_objects(db0_fixture):
    inner_memo = MemoTestClass("inner")
    db0_tuple = db0.tuple([inner_memo, "test", None])
    py_inner_memo = inner_memo
    py_tuple = (py_inner_memo, "test", None)
    assert str(db0_tuple) == str(py_tuple)
    assert repr(db0_tuple) == repr(py_tuple)

def test_db0_tuple_compare_with_other_types(db0_fixture):
    db0_tuple = db0.tuple([1, 2, 3])
    python_list = [1, 2, 3]
    python_set = {1, 2, 3}
    assert db0_tuple != python_list
    assert db0_tuple != python_set