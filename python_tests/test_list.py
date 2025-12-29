# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import itertools
import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass
import random


def make_python_list():
    return list()

def make_db0_list():
    return db0.list()

list_test_params = [(make_python_list), (make_db0_list)]

def test_db0_list_can_be_created(db0_fixture):
    list_1 = db0.list()    
    assert list_1 is not None    


def test_db0_list_can_be_created_with_params(db0_fixture):
    list_1 = db0.list([1,2,3])    
    assert list_1 is not None 
    assert len(list_1) == 3
    assert list_1 == [1,2,3]


@pytest.mark.parametrize("make_list", list_test_params)
def test_list_is_initially_empty(db0_fixture, make_list):
    list_1 = make_list()
    assert len(list_1) == 0

@pytest.mark.parametrize("make_list", list_test_params)
def test_list_can_append_none(db0_fixture, make_list):
    list_1 = make_list()
    assert len(list_1) == 0
    list_1.append(None)
    assert list_1[0] == None

@pytest.mark.parametrize("make_list", list_test_params)
def test_list_can_append_none(db0_fixture, make_list):
    list_1 = make_list()
    assert len(list_1) == 0
    list_1.append(None)
    assert list_1[0] == None


@pytest.mark.parametrize("make_list", list_test_params)
def test_list_items_access_by_index(db0_fixture, make_list):
    list_1 = make_list()
    list_1.append(1)
    list_1.append("hello")
    list_1.append(3.14)
    assert list_1[0] == 1
    assert list_1[1] == "hello"
    assert list_1[2] == 3.14
    assert list_1[-1] == 3.14
    assert list_1[-2] == "hello"
    assert list_1[-3] == 1


@pytest.mark.parametrize("make_list", list_test_params)
def test_list_items_can_be_updated_by_index(db0_fixture, make_list):
    list_1 = make_list()
    list_1.append(1)
    list_1.append(1)
    list_1[0] = 2
    assert list_1[0] == 2

@pytest.mark.parametrize("make_list", list_test_params)
def test_list_can_be_iterated(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    
    index = 0
    for number in list_1:
        assert number == index
        assert index < 5
        index += 1

@pytest.mark.parametrize("make_list", list_test_params)
def test_list_can_be_compared(db0_fixture, make_list):
    list_1 = make_list()
    list_2 = make_list()
    for i in range(10):
        list_1.append(i)
        list_2.append(i)
    assert list_1 == list_2
    list_2[5] = 17
    assert list_1 != list_2

@pytest.mark.parametrize("make_list", list_test_params)
def test_list_can_be_compared_to_pyton_list(db0_fixture, make_list):
    list_1 = make_list()
    list_2 = [1, 2]
    list_1.append(1)
    list_1.append(2)
    assert list_1 == list_2
    list_1[0]=17
    assert list_1 != list_2

@pytest.mark.parametrize("make_list", list_test_params)
def test_list_items_can_be_get_by_slice(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(10):
        list_1.append(i)
    assert len(list_1) == 10
    sublist = list_1[2:4]
    assert len(sublist) == 2
    assert sublist == [2,3]

    sublist = list_1[-2:]
    assert len(sublist) == 2
    assert sublist == [8,9]

    sublist = list_1[:-2]
    assert len(sublist) == 8
    assert sublist == [0,1,2,3,4,5,6,7]

    sublist = list_1[:-2:2]
    assert len(sublist) == 4
    assert sublist == [0,2,4,6]

    sublist = list_1[:-6:-1]
    assert len(sublist) == 5
    assert sublist == [9, 8, 7, 6, 5]

@pytest.mark.parametrize("make_list", list_test_params)
def test_list_slice_is_copy(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(10):
        list_1.append(i)
    assert len(list_1) == 10
    sublist = list_1[2:4]
    assert len(sublist) == 2
    sublist[0] = 5
    assert sublist[0] == 5
    assert list_1[2] == 2

@pytest.mark.parametrize("make_list", list_test_params)
def test_can_concat_list(db0_fixture, make_list):
    list_1 = make_list()
    list_2 = make_list()
    for i in range(5):
        list_1.append(i*2)
        list_2.append(i*2 + 1)
    assert len(list_1) == 5
    assert len(list_2) == 5
    list_3 = list_1 + list_2
    assert len(list_3) == 10
    assert list_3 == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]

@pytest.mark.parametrize("make_list", list_test_params)
def test_can_multiplicate_list(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    assert len(list_1) == 5
    list_3 = list_1 * 3
    assert len(list_3) == 15
    assert list_3 == [0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4]


@pytest.mark.parametrize("make_list", list_test_params)
def test_can_pop_from_list(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    assert len(list_1) == 5
    elem = list_1.pop()
    assert elem == 4
    assert len(list_1) == 4
    elem = list_1.pop(0)
    assert elem == 0
    assert len(list_1) == 3
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    elem = list_1.pop(3)
    assert len(list_1) == 4

@pytest.mark.parametrize("make_list", list_test_params)
def test_pop_from_list_deref_object(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    assert len(list_1) == 5
    elem = list_1.pop()
    assert elem == 4
    assert len(list_1) == 4
    elem = list_1.pop(0)
    assert elem == 0
    assert len(list_1) == 3
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    elem = list_1.pop(3)
    assert len(list_1) == 4


def test_can_clear_list(db0_fixture):
    list_1 = db0.list()
    ids = []
    for i in range(5):
        list_1.append(db0.tuple(["asd"]))

    tup1 = list_1.pop()
    tup2 = list_1.pop()    
    db0.commit()
    assert len(list_1) == 3

    
def test_clear_list_unref_objects(db0_fixture):    
    list_1 = db0.list()
    list_1.append(MemoTestClass("asd"))
    uuid = db0.uuid(list_1[0])
    list_1.clear()    
    db0.commit()    
    with pytest.raises(Exception):
        db0.fetch(uuid)


def test_set_item_to_list_unref_objects(db0_fixture):
    list_1 = db0.list()
    list_1.append(MemoTestClass("asd"))
    uuid = db0.uuid(list_1[0])
    list_1[0] = MemoTestClass("asd")
    uuid2 = db0.uuid(list_1[0])    
    db0.commit()
    with pytest.raises(Exception):
        db0.fetch(uuid)
    db0.fetch(uuid2)


def test_list_destroy_removes_reference(db0_fixture):
    obj = MemoTestClass(db0.list([MemoTestClass("asd")]))
    assert obj.value[0] is not None

    dep_uuid = db0.uuid(obj.value[0])
    db0.delete(obj)
    del obj
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)


@pytest.mark.parametrize("make_list", list_test_params)
def test_can_copy_list(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    assert len(list_1) == 5
    list_2 = list_1.copy()
    assert len(list_2) == 5
    assert list_1 == list_2
    list_1[0]= 17
    assert list_1 != list_2
    assert list_1[0] == 17
    assert list_2[0] == 0


@pytest.mark.parametrize("make_list", list_test_params)
def test_can_count_elements(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(10):
        list_1.append(i % 5)
    assert list_1.count(2) == 2

    for i in range(10):
        list_1.append(str(i % 3))
    assert list_1.count("2") == 3


@pytest.mark.parametrize("make_list", list_test_params)
def test_can_iterate_list(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    result = [0,1,2,3,4]
    index = 0
    for i in list_1:
        assert result[index] == i
        index += 1


@pytest.mark.parametrize("make_list", list_test_params)
def test_can_extend_list(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    assert len(list_1) == 5
    list_1.extend([1,2,3,4])
    assert len(list_1) == 9
    list_2 = make_list()
    for i in range(5):
        list_2.append(i)
    list_1.extend(list_2)
    assert len(list_1) == 14


@pytest.mark.parametrize("make_list", list_test_params)
def test_can_get_index(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    assert list_1.index(3) == 3
    list_1 = make_list()
    for i in range(5):
        list_1.append(str(i))
    assert list_1.index("2") == 2


@pytest.mark.parametrize("make_list", list_test_params)
def test_list_can_remove(db0_fixture, make_list):    
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    list_1.remove(3)
    assert len(list_1) == 4
    assert list_1 == [0, 1, 2, 4]
    list_1 = make_list()
    for i in range(5):
        list_1.append(str(i))
        
    list_1.remove("3")
    assert len(list_1) == 4
    assert list_1 == ["0", "1", "2", "4"]


@pytest.mark.parametrize("make_list", list_test_params)
def test_check_element_in_list(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)

    assert 2 in list_1
    assert 15 not in list_1


def test_db0_list_can_be_class_member(db0_fixture):
    object_1 = MemoTestClass(db0.list())
    object_1.value.append(1)
    assert object_1.value[0] == 1


def test_db0_list_can_be_retrieved_after_commit(db0_fixture):
    object_1 = MemoTestSingleton(db0.list())
    object_1.value.append(1)
    prefix_name = db0.get_prefix_of(object_1).name
    del object_1    
    
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    object_1 = MemoTestSingleton()    
    assert len(object_1.value) == 1    
    
    
def test_db0_list_is_always_pulled_through_py_cache(db0_fixture):    
    object_1 = MemoTestSingleton(db0.list())
    list_1 = object_1.value    
    object_2 = MemoTestSingleton()
    list_2 = object_2.value    
    # same db0 list instance
    assert list_2 is list_1
    del list_1    
    del list_2    
    
    
def test_db0_list_can_be_appended_after_commit(db0_fixture):
    object_1 = MemoTestSingleton(db0.list())
    for i in range(1):
        object_1.value.append(i)
    prefix_name = db0.get_prefix_of(object_1).name
    
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    object_1 = MemoTestSingleton()
    object_1.value.append(1)
    assert len(object_1.value) == 2


def test_db0_list_append_in_multiple_transactions(db0_fixture):
    cut = db0.list()
    cut.append(1)
    db0.commit()
    cut.append(1)    
    assert len(cut) == 2
    
    
@pytest.mark.stress_test
def test_list_append_stress_test(db0_fixture):
    append_count = 10000000
    cut = db0.list()
    for i in range(append_count):
        cut.append(i)
    
    # validate list's contents with random access
    for i in range(append_count):
        assert cut[i] == i
    assert len(cut) == append_count    


def test_list_drop_issue_1(db0_fixture):
    """
    This test was failing because db0 was trying to drop the list on final close
    even though the python reference was still accessible.
    """
    obj = MemoTestClass([1, 2, 3])
    list_obj = obj.value
    uuid = db0.uuid(obj)
    for i in range(5):
        list_obj.append(i)
    
    db0.commit()
    # list should NOT be dropped (since python reference is still accessible)
    assert list(db0.fetch(uuid).value) == [1, 2, 3, 0, 1, 2, 3, 4]
    
    
def test_list_index_access_issue_1(db0_fixture):
    """
    Issue: the access by-index was requesting read-write access to the underlying prefix
    Error: the test was failing with: Cannot modify read-only prefix: my-test-prefix
    """
    px_name = db0.get_current_prefix().name
    _ = MemoTestSingleton([1, 2, 3])
    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    root = MemoTestSingleton()
    assert root.value[0] == 1


def test_list_pop_while_iteration(db0_fixture):
    """
    Issue: https://github.com/wskozlowski/dbzero/issues/252
    The test was failing with: segmentation fault
    """
    cut = db0.list([0, 1, 2, 3, 4])
    for i in cut:
        cut.pop()
    assert len(cut) == 2


def test_list_bool_storage(db0_fixture):
    cut = db0.list([True, False, True, False, None])    
    assert cut == [True, False, True, False, None]
    
    
def test_sliced_list_bool_storage(db0_fixture):
    cut = db0.list([True, False, True, False, None])
    assert cut[0:3] == [True, False, True]


def test_list_extend_with_none(db0_fixture):
    cut = db0.list([])
    cut.extend([None] * 1024)
    for i in range(1024):
        assert cut[i] is None


def test_db0_list_str_same_as_python_list(db0_fixture):
    db0_list = db0.list([1, "two", 3.0, None])
    py_list = [1, "two", 3.0, None]
    assert str(db0_list) == str(py_list)
    assert repr(db0_list) == repr(py_list)


def test_db0_list_str_with_nested_objects(db0_fixture):
    inner_list = db0.list([1, 2, 3])
    db0_list = db0.list([inner_list, "test", None])
    py_inner_list = [1, 2, 3]
    py_list = [py_inner_list, "test", None]
    assert str(db0_list) == str(py_list)
    assert repr(db0_list) == repr(py_list)


def test_db0_list_str_with_nested_memo_objects(db0_fixture):
    inner_memo = MemoTestClass("inner")
    db0_list = db0.list([inner_memo, "test", None])
    py_inner_memo = inner_memo
    py_list = [py_inner_memo, "test", None]
    assert str(db0_list) == str(py_list)
    assert repr(db0_list) == repr(py_list)


def test_db0_list_islice_iteration(db0_fixture):
    db0_list = db0.list(range(30))
    expected_values = [10, 12, 14, 16, 18]
    for index, value in enumerate(itertools.islice(db0_list, 10, 20, 2)):    
        assert value == expected_values[index]


def test_db0_list_compare_with_other_typse(db0_fixture):
    db0_list = db0.list([1, 2, 3])
    python_tuple = (1, 2, 3)
    python_set = {1, 2, 3}
    assert db0_list != python_tuple
    assert db0_list != python_set
    
    
@pytest.mark.stress_test
@pytest.mark.parametrize("db0_autocommit_fixture", [50], indirect=True)
def test_append_to_random_lists(db0_autocommit_fixture):
    print("Creating multiple lists")
    db0.set_cache_size(8 << 30)
    lists = db0.dict()
    for k in range(100000):
        lists[k] = db0.index()
    
    RANDOM_BYTES = b'DB0'*22000
    count = 0
    db0.commit()
    print(f"Appending objects to random {len(lists)} lists")
    for _ in range(200000):
        item = lists[random.randint(0, len(lists) - 1)]
        if random.randint(0, 100) < 10:
            # 20% chance to create a large object
            data_size = random.randint(8000, 56000)
        else:
            # mostly create small objects
            data_size = random.randint(1, 1500)

        item.add(count, MemoTestClass(value = RANDOM_BYTES[0:data_size]))
        count += 1
        if count % 5000 == 0:
            db0.commit()
        if count % 10000 == 0:
            print(f"Appended {count} objects")
            print(f"Prefix size = {db0.get_storage_stats()['prefix_size']} bytes")
    
    db0.commit()


def test_list_tuple_indexed_access(db0_fixture):
    db0_list = db0.list([1, 2, 3, 5, 6, 7, 8])
    assert db0_list[(1, 3, 4)] == [2, 5, 6]
