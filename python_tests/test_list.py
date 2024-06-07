import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


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
def test_can_clear_list(db0_fixture, make_list):
    list_1 = make_list()
    for i in range(5):
        list_1.append(i)
    assert len(list_1) == 5
    list_1.clear()
    assert len(list_1) == 0
    list_1.append(1)
    assert len(list_1) == 1
    list_1.clear()
    assert len(list_1) == 0


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
    try:
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
    except Exception as ex:
        print(ex)


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
    prefix_name = db0.get_prefix(object_1)
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
    prefix_name = db0.get_prefix(object_1)
    
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
