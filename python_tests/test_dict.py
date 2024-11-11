import pytest
import random
import datetime
import dbzero_ce as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestSingleton, MemoTestClass


def test_can_create_dict(db0_fixture):
    dict_1 = db0.dict()
    assert dict_1 != None


def make_python_dict(*args, **kwargs):
    return dict(*args, **kwargs)

def make_db0_dict(*args, **kwargs):
    return db0.dict(*args, **kwargs)

dict_test_params = [(make_python_dict), (make_db0_dict)]

@pytest.mark.parametrize("make_dict", dict_test_params)
def test_can_create_dict_from_array(db0_fixture, make_dict):
    dict_1 = make_dict([("item", 2), ("item_2", 3)])
    assert dict_1 != None
    assert dict_1["item"] == 2
    assert dict_1["item_2"] == 3


@pytest.mark.parametrize("make_dict", dict_test_params)
def test_can_create_dict_from_kwargs(db0_fixture, make_dict):
    dict_1 = make_dict(item=2, item_2=3, item_3=6)
    assert dict_1 != None
    assert dict_1["item"] == 2
    assert dict_1["item_2"] == 3
    assert dict_1["item_3"] == 6

@pytest.mark.parametrize("make_dict", dict_test_params)
def test_can_dict_assign_item(db0_fixture, make_dict):
    dict_1 = make_dict()
    dict_1["item"] = 5
    assert dict_1["item"] == 5
    dict_1["item"] = 7
    assert dict_1["item"] == 7


@pytest.mark.parametrize("make_dict", dict_test_params)
def test_constructors(db0_no_autocommit, make_dict):
    a = make_dict(one=1, two=2, three=3)
    b = make_dict({'one': 1, 'three': 3}, two=2)
    c = make_dict(zip(['one', 'two', 'three'], [1, 2, 3]))
    d = make_dict([('two', 2), ('one', 1), ('three', 3)])
    e = make_dict({'three': 3, 'one': 1, 'two': 2})
    a == b == c == d == e


@pytest.mark.parametrize("make_dict", dict_test_params)
def test_can_check_in_dict(db0_fixture, make_dict):
    dict_1 = make_dict(item=2, item_2=3)
    assert dict_1 != None
    assert "item" in dict_1
    assert "item5" not in dict_1

@pytest.mark.parametrize("make_dict", dict_test_params)
def test_can_iterate_over_dict(db0_fixture, make_dict):
    dict_1 = make_dict(item=2, item_2=3, three=3)
    keys_expected = ["item", "item_2", "three"]
    keys_expected.sort()
    keys = []
    for key in dict_1:
        keys.append(key)
    keys.sort()
    assert keys == keys_expected

    
@pytest.mark.parametrize("make_dict", dict_test_params)
def test_can_create_list_of_keys_from_dict(db0_fixture, make_dict):
    dict_1 = make_dict(item=2, item_2=3, three=3)
    keys_expected = ["item", "item_2", "three"]
    keys_expected.sort()
    keys = list(dict_1)
    keys.sort()
    assert keys == keys_expected


@pytest.mark.parametrize("make_dict", dict_test_params)
def test_can_clear_dictionary(db0_fixture, make_dict):
    dict_1 = make_dict(item=2, item_2=3, three=3)
    assert len(dict_1) == 3
    dict_1.clear()
    assert len(dict_1) == 0

@pytest.mark.parametrize("make_dict", dict_test_params)
def test_can_copy_dictionary(db0_fixture, make_dict):
    dict_1 = make_dict(item=2, item_2=3, three=3)
    assert dict_1["item"] == 2
    dict_2 = dict_1.copy()
    dict_1["item"] = 6
    assert dict_1["item"] == 6
    assert dict_2["item"] == 2

@pytest.mark.parametrize("make_dict", dict_test_params)
def test_dict_from_key(db0_fixture, make_dict):
    dict_1 = make_dict().fromkeys(["key_1", "key_2", "key_3"], 5)
    assert len(dict_1) == 3
    assert dict_1["key_1"] == 5
    assert dict_1["key_2"] == 5
    assert dict_1["key_3"] == 5

    dict_1 = make_dict().fromkeys(["key_1", "key_2", "key_3"])
    assert len(dict_1) == 3
    assert dict_1["key_1"] == None
    assert dict_1["key_2"] == None
    assert dict_1["key_3"] == None


@pytest.mark.parametrize("make_dict", dict_test_params)
def test_get_from_dict(db0_fixture, make_dict):
    dict_1 = make_dict([("item", 2), ("item_2", 3)])
    assert dict_1.get("item") == 2
    assert dict_1.get("item_2", "default") == 3
    assert dict_1.get("item_3", "default") == "default"
    assert dict_1.get("item_3") == None

@pytest.mark.parametrize("make_dict", dict_test_params)
def test_pop_from_dict(db0_fixture, make_dict):
    dict_1 = make_dict([("item", 2), ("item_2", 3), ("item_3", 3)])
    assert dict_1.pop("item") == 2
    assert "item" not in dict_1
    with pytest.raises(KeyError):
        dict_1.pop("item")
    assert dict_1.pop("item_2", "default") == 3
    assert "item_2" not in dict_1
    assert dict_1.pop("item_2", "default") == "default"

@pytest.mark.parametrize("make_dict", dict_test_params)
def test_set_default(db0_fixture, make_dict):
    dict_1 = make_dict([("item", 2), ("item_2", 3)])
    assert dict_1.setdefault("item") == 2
    assert dict_1.setdefault("item_2", "default") == 3
    assert "item_3" not in dict_1
    assert dict_1.setdefault("item_3", "default") == "default"
    assert "item_3" in dict_1
    assert dict_1.setdefault("item_3") == "default"
    assert dict_1.setdefault("item_4") == None

@pytest.mark.parametrize("make_dict", dict_test_params)
def test_update_dict(db0_fixture, make_dict):
    dict_1 = make_dict([("item", 2), ("item_2", 3), ("item_3", 3)])
    dict_1.update({"item": 7}, item_2=5)
    assert dict_1["item"] == 7
    assert dict_1["item_2"] == 5
    dict_2 = make_dict([("item_3", 9)])
    dict_1.update(dict_2)
    assert dict_1["item_3"] == 9


@pytest.mark.parametrize("make_dict", dict_test_params)
def test_items_from_dict(db0_fixture, make_dict):
    # tests iteration over items from dict
    dict_1 = make_dict([("item", 2), ("item_2", 3), ("item_3", 3)])
    items = dict_1.items()
    assert len(items) == 3
    for key, value in items:
        assert dict_1[key] == value
    dict_1["items_4"] = 18
    assert len(items) == 4


@pytest.mark.parametrize("make_dict", dict_test_params)
def test_keys_from_dict(db0_fixture, make_dict):
    # tests iteration over keys from dict
    dict_1 = make_dict([("item", 2), ("item_2", 3), ("item_3", 3)])
    keys = dict_1.keys()
    assert len(keys) == 3
    result = []
    for key in keys:
        result.append(key)
    result.sort()
    assert result == ["item", "item_2", "item_3"]
    dict_1["items_4"] = 18
    assert len(keys) == 4
    assert "items_4" in keys


@pytest.mark.parametrize("make_dict", dict_test_params)
def test_values_from_dict(db0_fixture, make_dict):
    # tests iteration over values from dict
    dict_1 = make_dict([("item", 2), ("item_2", 3), ("item_3", 3)])
    values = dict_1.values()
    assert len(values) == 3
    result = []
    for value in values:
        result.append(value)
    result.sort()
    assert result == [2, 3, 3]
    dict_1["items_4"] = 18
    assert len(values) == 4
    assert 18 in values
    
    
def test_dict_not_persisting_keys_issue(db0_fixture):
    root = MemoTestSingleton({})
    my_dict = root.value
    value = my_dict.get("first", None)
    assert value is None    
    prefix = db0.get_current_prefix()
    my_dict["first"] = MemoTestClass("abc")
    my_dict["second"] = MemoTestClass("222")
    my_dict["third"] = MemoTestClass("333")
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    # open as read-write
    db0.open(prefix.name)
    root = MemoTestSingleton()
    my_dict = root.value
    value = my_dict.get("third", None)
    assert value.value == "333"


def test_dict_with_dicts_as_value(db0_fixture):
    my_dict = db0.dict()
    my_dict["asd"] = {"first": 1}
    for key, item in my_dict.items():
        assert item["first"] == 1


def test_dict_with_tuples_as_keys(db0_no_autocommit):
    my_dict = db0.dict()
    my_dict[("first", 1)] = MemoTestClass(1)
    for item in my_dict.items():
        pass


def test_dict_with_unhashable_types_as_keys(db0_fixture):
    my_dict = db0.dict()
    with pytest.raises(Exception) as ex:
        my_dict[["first", 1]] = MemoTestClass("abc")

    assert "unhashable type" in str(ex.value)

    with pytest.raises(Exception) as ex:
        my_dict[{"key":"value"}] = MemoTestClass("abc")

    assert "unhashable type" in str(ex.value)


def test_dict_items_in(db0_no_autocommit):
    # tests iteration over values from dict
    dict_1 = db0.dict()
    # insert 1000 random items
    for i in range(100):
        dict_1[i] = i
    assert len(dict_1) == 100
    now = datetime.datetime.now()
    for i in range(100000):
        random_int = random.randint(0, 300)
        if random_int < 100:
            assert random_int in dict_1
        else:
            assert random_int not in dict_1
    end = datetime.datetime.now()
    print("Elapsed time: ", end - now)


def test_dict_insert_mixed_types_issue_1(db0_fixture):
    """
    This test was failing with "invalid address" due to memo wrappers not being destroyed
    in fact the memo object will be destroyed by Python after db0.close() and will be persisted in db0
    without references
    """
    my_dict = db0.dict()
    my_dict["abc"] = { 0: MemoTestClass("abc") }


def test_dict_insert_mixed_types_v1(db0_fixture):
    my_dict = db0.dict(
        { "abc": (123, { "a": MemoTestClass("abc"), "b": MemoTestClass("def") }),
          "def": (123, { "a": MemoTestClass("abc"), "b": MemoTestClass("def") }) })
    assert len(my_dict) == 2

    
def test_dict_insert_mixed_types_v2(db0_fixture):
    my_dict = db0.dict()
    my_dict["abc"] = (123, { "a": MemoTestClass("abc"), "b": MemoTestClass("def") })
    my_dict["def"] = (123, { "a": MemoTestClass("abc"), "b": MemoTestClass("def") })
    assert len(my_dict) == 2
    

def test_dict_with_tuples_as_values(db0_fixture):
    my_dict = db0.dict()
    my_dict[1] = (1, "first")
    my_dict[2] = (2, "second")
    for key, item in my_dict.items():
        assert key in [1, 2]
        assert item[0] in [1, 2]
        assert item[1] in ["first", "second"]


def test_dict_values(db0_fixture):
    my_dict = db0.dict()
    my_dict[1] = (1, "first")
    my_dict[2] = (2, "second")
    count = 0
    for value in my_dict.values():        
        assert value[0] in [1, 2]
        assert value[1] in ["first", "second"]
        count += 1
    assert count == 2


def test_unpack_tuple_element(db0_fixture):
    my_dict = db0.dict()
    my_dict[1] = (1, b"bytes", "first")    
    a, b, c = my_dict[1]
    assert a == 1
    assert b == b"bytes"
    assert c == "first"

def test_clear_unref_keys_and_values(db0_fixture):
    my_dict = db0.dict()
    key = MemoTestClass("key")
    my_dict[key] = MemoTestClass("Value")
    uuid_value = db0.uuid(my_dict[key])
    uuid_key = db0.uuid(key)
    key = None
    my_dict.clear()
    db0.clear_cache()
    db0.commit()
    with pytest.raises(Exception):
        db0.fetch(uuid_value)
    with pytest.raises(Exception):
        db0.fetch(uuid_key)


def test_pop_unref_and_values(db0_fixture):
    my_dict = db0.dict()
    key = MemoTestClass("key")
    my_dict[key] = MemoTestClass("Value")
    uuid_value = db0.uuid(my_dict[key])
    uuid_key = db0.uuid(key)
    my_dict.pop(key)
    key = None
    db0.clear_cache()
    db0.commit()
    with pytest.raises(Exception):
        db0.fetch(uuid_value)
    print("HERE 10")
    with pytest.raises(Exception):
        db0.fetch(uuid_key)


def test_dict_destroy_issue_1(db0_no_autocommit):
    """
    This test is failing with segfault due to use of dict's [] operator
    it succeeds when we relace the use of [] operator with iteration
    the problem was with Dict::commit not being called
    """
    obj = MemoTestClass(db0.dict({0: MemoTestClass("value")}))
    db0.commit()
    _ = db0.uuid(obj.value[0])
    del obj
        

def test_dict_destroy_issue_2(db0_no_autocommit):
    """
    This was is failing with segfault due to use of dict's [] operator
    it succeeds when we relace the use of [] operator with iteration
    the problem was with Dict::commit not being called
    """
    obj = db0.dict()
    obj[0] = 0
    db0.commit()
    _ = obj[0]
    del obj

    
def test_dict_destroy_removes_reference(db0_fixture):
    key = MemoTestClass("asd")
    obj = MemoTestClass(db0.dict({key: MemoTestClass("value")}))
    db0.commit()
    value_uuid = db0.uuid(obj.value[key])
    key_uuid = db0.uuid(key)
    key = None
    db0.delete(obj)
    del obj
    db0.clear_cache()
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(key_uuid)
    with pytest.raises(Exception):
        db0.fetch(value_uuid)
    
    
def test_make_dict_issue_1(db0_no_autocommit):
    """
    The test was failing with segfault
    """
    _ = [make_db0_dict() for _ in range(6)]


# FIXME: failing test blocked
# def test_dict_in_dict_issue1(db0_no_autocommit):
#     d1 = db0.dict()
#     d1["value"] = {}
#     assert d1.keys() == ["value"]
