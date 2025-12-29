# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import random
import datetime
import dbzero as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestSingleton, MemoTestClass, MemoScopedSingleton, MemoScopedClass, MonthTag, DATA_PX
from decimal import Decimal


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
        assert type(item) is tuple
    

def test_dict_with_unhashable_types_as_keys(db0_fixture):
    my_dict = db0.dict()
    with pytest.raises(Exception) as ex:
        my_dict[["first", 1]] = MemoTestClass("abc")    
    assert "hash" in str(ex.value)    
    with pytest.raises(Exception) as ex:
        my_dict[{"key":"value"}] = MemoTestClass("abc")    
    assert "hash" in str(ex.value)


def test_dict_items_in(db0_no_autocommit):
    # tests iteration over values from dict
    dict_1 = db0.dict()
    # insert 1000 random items
    for i in range(100):
        dict_1[i] = i
    assert len(dict_1) == 100    
    for i in range(100000):
        random_int = random.randint(0, 300)
        if random_int < 100:
            assert random_int in dict_1
        else:
            assert random_int not in dict_1
    

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
    del key
    db0.commit()
    with pytest.raises(Exception):
        db0.fetch(uuid_value)    
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


def test_dict_in_dict_issue1(db0_no_autocommit):
    d1 = db0.dict()
    d1["value"] = {}
    assert list(d1.keys()) == ["value"]


def test_dict_duplicate_keys_issue1(db0_no_autocommit):
    current_prefix = db0.get_current_prefix()
    db0.open("my-new-prefix")
    # go back to current prefix but use the other prefix as a scope
    db0.open(current_prefix.name)
    mtc = MemoScopedSingleton({}, prefix="my-new-prefix")
    key = "5HZSGQCE767XN6YXYO626RTY2EDLDWGHQ6ILXVAB7QMRO4S4E3ZA"
    index = 0
    for _ in range(10):
        if key not in mtc.value:
            mtc.value[key] = {}
        mtc.value[key][f"index_{index}"] = (1, 2, "abc")
        index += 1
    
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-new-prefix", "r")
    mtc = MemoScopedSingleton({}, prefix="my-new-prefix")    
    assert mtc.value.get(key, None) is not None
    assert len(mtc.value) == 1
    assert len(mtc.value[key]) == 10


def test_dict_no_duplicate_keys_when_mixed_python_db0_types(db0_no_autocommit):
    Colors = db0.enum("Colors", values = ["RED", "GREEN", "BLUE"])
    d1 = db0.dict()
    d2 = {}
    
    def rand_string():
        return "".join([chr(random.randint(65, 90)) for _ in range(10)])
    
    all_keys = [rand_string() for i in range(200)]    
    def update(d, keys):
        for key in keys:
            value = d.get((key, Colors.RED), None)
            if value is not None:
                d[(key, Colors.RED)] = value + 1
            else:
                d[(key, Colors.RED)] = 1

    for _ in range(10):
        keys = [random.choice(all_keys) for _ in range(10)]        
        update(d1, keys)
        update(d2, keys)    
        assert len(d1) == len(d2)
    

def test_dict_storing_enum_values_from_different_prefix(db0_no_autocommit):
    # create data_px since MonthTag enum is scoped to it
    db0.open(DATA_PX, "rw")
    obj = db0.dict()
    # NOTE: MonthTag is located on a different data-prefix
    obj[(MonthTag.NOV, 1, "Szczecin")] = 1
    obj[MonthTag.NOV] = 2
    obj[MonthTag.NOV] = 3
    obj[(MonthTag.NOV, 1, "Szczecin")] = 2
    assert len(obj) == 2
    

@pytest.mark.parametrize("make_dict", dict_test_params)
def test_dict_raises_key_error(db0_fixture, make_dict):
    dict_1 = make_dict([("item", 2), ("item_2", 3)])
    with pytest.raises(KeyError):
        dict_1['item_3']
        
        
def test_pydict_with_db0_tuples_as_keys(db0_no_autocommit):
    py_dict = {}
    t1 = db0.tuple(["first", 1])
    with pytest.raises(Exception) as ex:
        py_dict[t1] = MemoTestClass(1)
    assert "unhashable" in str(ex.value)


def test_dict_del_by_key(db0_no_autocommit):
    cut = db0.dict({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
    del cut["a"]
    assert len(cut) == 4
    

def test_dict_del_items_while_iterating_over(db0_no_autocommit):
    cut = db0.dict({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})
    keys = iter(list(cut.keys()))
    for item in cut.items():
        cut.pop(next(keys))
    
    assert len(cut) > 0
    
    
def test_dict_pop_tuple_keys(db0_no_autocommit):
    keys = [(MemoTestClass(i), Decimal(i)) for i in range(100)]
    cut = db0.dict({k: "value" for k in keys})
    assert len(cut) == 100
    
    to_remove =  [21, 7, 48, 23, 24, 98, 12, 58, 79, 39, 17, 81, 36, 89, 72, 6, 54, 97, 
                  8, 50, 77, 34, 0, 62, 14, 44, 95, 86, 73, 18, 45, 53, 84, 38, 64, 60, 
                  68, 28, 75, 40, 22, 49, 85, 63, 2, 15, 4, 66, 16, 80]
    for index in to_remove:
        cut.pop(keys[index])

    assert len(cut) == 50
    
    
def test_dict_pop_complex_objects(db0_no_autocommit):
    keys = [(MemoTestClass(i), Decimal(i)) for i in range(100)]
    cut = db0.dict({k: MemoTestClass(MemoTestClass(k)) for k in keys})
    assert len(cut) == 100
    
    to_remove =  [21, 7, 48, 23, 24, 98, 12, 58, 79, 39, 17, 81, 36, 89, 72, 6, 54, 97, 
                  8, 50, 77, 34, 0, 62, 14, 44, 95, 86, 73, 18, 45, 53, 84, 38, 64, 60, 
                  68, 28, 75, 40, 22, 49, 85, 63, 2, 15, 4, 66, 16, 80]
    removed = []
    for index in to_remove:
        removed.append(cut.pop(keys[index]))
    
    assert len(cut) == 50
    for item in removed:
        assert isinstance(item, MemoTestClass)
        assert isinstance(item.value, MemoTestClass)        
    

def test_dict_pop_issue1(db0_no_autocommit):
    """
    Issue: this test was causing segfault once in a while
    Resolution: bug in db0.hash implementation for db0.Tuple (retrieving a pointer to a temporary object)
    """
    cut = db0.dict([((0, Decimal(i)), 0) for i in range(50)])
    for key in cut.keys():
        _ = db0.hash(key)


def test_using_type_as_dict_key(db0_no_autocommit):
    cut = db0.dict()
    cut[MemoTestClass] = "first item"
    cut[MemoTestSingleton] = "second item"
    assert cut[MemoTestClass] == "first item"
    assert cut[MemoTestSingleton] == "second item"
    
    
def test_using_mixed_type_object_keys(db0_no_autocommit):
    keys = [MemoTestClass, MemoTestClass(123), MemoTestSingleton, MemoTestSingleton(999)]
    cut = db0.dict({key: index for index, key in enumerate(keys)})
    assert cut[MemoTestClass] == 0
    assert cut[MemoTestSingleton] == 2
    

def test_dict_del_key(db0_no_autocommit):    
    cut = db0.dict({"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})    
    del cut["a"]    
    assert len(cut) == 4
    assert "a" not in cut


def test_db0_dict_str_same_as_python_dict(db0_fixture):
    db0_dict = db0.dict({"one": 1, "two": "two", "three": 3.0, "four": None})
    py_dict = {"one": 1, "two": "two", "three": 3.0, "four": None}
    # Since dicts are unordered, we need to compare sorted string representations
    # to ensure consistent comparison regardless of key order
    db0_str = str(sorted(str(db0_dict).replace("{", "").replace("}", "").split(", ")))
    py_str = str(sorted(str(py_dict).replace("{", "").replace("}", "").split(", ")))
    assert db0_str == py_str

def test_db0_dict_str_with_nested_objects(db0_fixture):
    inner_dict = db0.dict({"a": 1, "b": 2, "c": 3})
    db0_dict = db0.dict({"nested": inner_dict, "text": "test", "none_val": None})
    py_inner_dict = {"a": 1, "b": 2, "c": 3}
    py_dict = {"nested": py_inner_dict, "text": "test", "none_val": None}
    # Compare string representations by checking if they contain the same elements
    db0_dict_str = str(db0_dict)
    py_dict_str = str(py_dict)
    # Check that both string representations are valid dict strings
    assert db0_dict_str.startswith("{") and db0_dict_str.endswith("}")
    assert py_dict_str.startswith("{") and py_dict_str.endswith("}")
    # Verify the nested structure matches
    assert "'text': 'test'" in db0_dict_str
    assert "'none_val': None" in db0_dict_str

def test_db0_dict_str_with_nested_memo_objects(db0_fixture):
    inner_memo = MemoTestClass("inner")
    db0_dict = db0.dict({"memo": inner_memo, "text": "test", "none_val": None})
    py_dict = {"memo": inner_memo, "text": "test", "none_val": None}
    # Compare string representations
    db0_dict_str = str(db0_dict)
    py_dict_str = str(py_dict)
    # Both should be valid dict string representations
    assert db0_dict_str.startswith("{") and db0_dict_str.endswith("}")
    assert py_dict_str.startswith("{") and py_dict_str.endswith("}")
    # Check that the memo object appears in both string representations
    memo_str = repr(inner_memo)
    assert memo_str in db0_dict_str
    assert memo_str in py_dict_str