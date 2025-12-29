# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import (MemoTestClass, MemoTestThreeParamsClass, MemoTestCustomLoadClass, 
                              MemoTestCustomLoadClassWithParams, MemoTestSingleton)


def test_load_py_string():  
    assert db0.load("abc") == "abc"


def test_load_py_int():
    assert db0.load(123) == 123


def test_load_tuple(db0_fixture):
    t1 = db0.tuple([1, "string", 999])
    assert db0.load(t1) == (1, "string", 999)


def test_load_py_tuple_of_db0_enums(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    t1 = (Colors.RED, Colors.GREEN)
    assert db0.load(t1) == ("RED", "GREEN")


def test_load_list(db0_fixture):
    t1 = db0.list([1, "string", 999])
    assert db0.load(t1) == [1, "string", 999]


def test_py_load_list(db0_fixture):
    t1 = [1, "string", 999]
    assert db0.load(t1) == [1, "string", 999]


def test_load_py_tuple_of_db0_classes(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])    
    memo = MemoTestClass("string")
    t1 = (Colors.RED, Colors.GREEN, memo)
    assert db0.load(t1) == ("RED", "GREEN", {"value": "string"})


def test_load_memo_types(db0_fixture):
    memo = MemoTestClass("string")
    assert db0.load(memo) == {"value": "string"}


def test_load_db0_list(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])        
    cut = ["1", 2 , Colors.GREEN]
    assert db0.load(cut) == ["1", 2, "GREEN"]


def test_load_memo_db0_types(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    t1 = (Colors.RED, Colors.GREEN)
    list = ["1", 2 , Colors.GREEN]
    memo2 = MemoTestClass("string")
    memo = MemoTestThreeParamsClass(t1, list, memo2)
    assert db0.load(memo) == {
        "value_1": ("RED", "GREEN"),
        "value_2": ["1", 2, "GREEN"],
        "value_3": {"value": "string"}
    }


def test_load_dict(db0_fixture):
    t1 = db0.dict({"key1": 1, "key2": "string", "key3": 999})
    assert db0.load(t1) == {"key1": 1, "key2": "string", "key3": 999}


def test_load_py_dict(db0_fixture):
    t1 = {"key1": 1, "key2": "string", "key3": 999}
    assert db0.load(t1) == {"key1": 1, "key2": "string", "key3": 999}


def test_load_set(db0_fixture):
    t1 = db0.set([1, "string", 999])
    loaded = db0.load(t1)
    assert len(loaded) == 3
    for item in t1:
        assert item in loaded


def test_load_py_set(db0_fixture):
    t1 = {1, "string", 999}
    loaded = db0.load(t1)
    assert len(loaded) == 3
    for item in t1:
        assert item in loaded


def test_load_returns_none_not_as_string(db0_fixture):
    assert db0.load(None) == None
    t1 = db0.set([1, "string", None])
    loaded = db0.load(t1)
    assert len(loaded) == 3
    assert None in loaded
    assert 1 in loaded
    assert "string" in loaded


def test_load_with_default_load_method(db0_fixture):
    memo = MemoTestCustomLoadClass("value_1", "value_2", "value_3")
    assert db0.load(memo) == {
            "v1": "value_1",
            "v2_v3": {"value_2": "value_3"}
        }


def test_load_with_default_load_as_subclas(db0_fixture):

    memo = MemoTestCustomLoadClass("value_1", "value_2", "value_3")
    t1 = db0.dict({"key1": 1, "key2": "string", "key3": memo})
    assert db0.load(t1) == {
        "key1": 1,
        "key2": "string",
        "key3": {"v1": "value_1", "v2_v3": {"value_2": "value_3"}},
    }


def test_load_with_default_load_as_subclas(db0_fixture):

    memo = MemoTestCustomLoadClass("value_1", "value_2", "value_3")
    memo2 = MemoTestCustomLoadClass("value_1", "value_2", memo)
    assert db0.load(memo2) == {
        "v1": "value_1",
        "v2_v3": {"value_2": {"v1": "value_1", "v2_v3": {"value_2": "value_3"}}},
    }


def test_load_exclude(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    t1 = (Colors.RED, Colors.GREEN)
    list = ["1", 2 , Colors.GREEN]
    memo2 = MemoTestClass("string")
    memo = MemoTestThreeParamsClass(t1, list, memo2)
    assert db0.load(memo, exclude = ["value_1"]) == {
        "value_2": ["1", 2, "GREEN"],
        "value_3": {"value": "string"}
    }


def test_load_exclude_doesnt_exclude_in_subobject(db0_fixture):
    memo = MemoTestThreeParamsClass("value_1", "value_2", "value_3")
    memo2 = MemoTestThreeParamsClass("value_1", "value_2", memo)
    assert db0.load(memo2, exclude = ["value_1"]) == {
        "value_2": "value_2",
        "value_3": {
            "value_1": "value_1",
            "value_2": "value_2",
            "value_3": "value_3"
        }
    }


def test_load_exclude_doesnt_work_with_added_load_method(db0_fixture):
    memo = MemoTestCustomLoadClass("value_1", "value_2", "value_3")

    with pytest.raises(AttributeError) as ex:
         db0.load(memo, exclude = ["value_1"])
    assert "Cannot exclude values when __load__ is implemented" in str(ex.value)


def test_load_exclude_only_supports_list(db0_fixture):
    memo = MemoTestThreeParamsClass("value_1", "value_2", "value_3")

    assert db0.load(memo, exclude = ["value_1"]) == {"value_2": "value_2", "value_3": "value_3"}
    assert db0.load(memo, exclude = ("value_1",)) == {"value_2": "value_2", "value_3": "value_3"}
    assert db0.load(memo, exclude = db0.list(["value_1"])) == {"value_2": "value_2", "value_3": "value_3"}
    assert db0.load(memo, exclude = db0.tuple(["value_1"])) == {"value_2": "value_2", "value_3": "value_3"}
    with pytest.raises(TypeError) as ex:
         db0.load(memo, exclude = "value_1")
    assert "Invalid argument type. Exclude shoud be a sequence" in str(ex.value)
    with pytest.raises(TypeError) as ex:
        db0.load(memo, exclude = {"value_1":"value2"})
    assert "Invalid argument type. Exclude shoud be a sequence" in str(ex.value)


def test_load_can_get_kwargs(db0_fixture):
    memo = MemoTestCustomLoadClassWithParams("value_1", "value_2", "value_3")

    loaded = db0.load(memo, param="some param")
    assert "param" in loaded
    assert loaded["param"] == "some param"


def test_load_can_run_with_custom_load_without_kwargs(db0_fixture):
    memo = MemoTestCustomLoadClassWithParams("value_1", "value_2", "value_3")

    loaded = db0.load(memo, param="some param", other_non_used="other")
    assert "param" in loaded
    assert loaded["param"] == "some param"


def test_load_can_load_keys_in_dict(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    test_dict = {
        Colors.RED: "red",
    }

    loaded = db0.load(test_dict)
    assert loaded == {"RED": "red"}

def test_load_can_load_keys_in_db0_dict(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    test_dict = db0.dict({
        Colors.RED: "red",
    })

    loaded = db0.load(test_dict)
    assert loaded == {"RED": "red"}


def test_load_set_of_tuples_issue1(db0_fixture):
    """
    Issue: https://github.com/wskozlowski/dbzero/issues/223
    """
    obj = db0.set([(1,2), (2,3)])
    lodaded = db0.load(obj)
    assert len(lodaded) == 2
    for item in obj:
        assert item in lodaded

def test_load_cyclic_graph_issue1(db0_fixture):
    """
    Issue: https://github.com/wskozlowski/dbzero/issues/338
    """
    root = MemoTestSingleton({})
    root.value["a"] = MemoTestClass(123)
    root.value["b"] = MemoTestClass({})
    root.value["b"].value["x"] = MemoTestClass(456)
    root.value["c"] = root
    
    with pytest.raises(RecursionError):
        db0.load(root)


def test_load_set_of_objects(db0_fixture):
    set_of_objects = db0.set([MemoTestClass("a"), MemoTestClass("b")])
    loaded = db0.load(set_of_objects)
    assert loaded == [{"value": "a"}, {"value": "b"}]

class TestObject:
    __test__ = False

    def __init__(self, value, other_value):
        self.value = value
        self.other_value = other_value

    def __load__(self, **kwargs):
        return {
            "value": self.value,
            "other_value": self.other_value
        }

def test_load_python_object(db0_fixture):
    test_object = TestObject("a", "b")
    loaded = db0.load(test_object)
    assert loaded == {"value": "a", "other_value": "b"}


@db0.memo
class TestObjectNoLoad:
    __test__ = False

    def __init__(self, value, other_value):
        self.value = value
        self.other_value = other_value


@db0.memo
class DerivedTestObject(TestObjectNoLoad):
    def __init__(self, value, other_value, extra):
        super().__init__(value, other_value)
        self.extra = extra
        self.more_extra = None


def test_load_with_inheritance_and_mutations(db0_fixture):
    obj = DerivedTestObject(None, None, "c")    
    assert db0.load(obj) == {"value": None, "other_value": None, "extra": "c", "more_extra": None}    
    obj.value = "a"
    assert db0.load(obj) == {"value": "a", "other_value": None, "extra": "c", "more_extra": None}
    obj.other_value = "b"   
    assert db0.load(obj) == {"value": "a", "other_value": "b", "extra": "c", "more_extra": None}
    obj.more_extra = "d"
    assert db0.load(obj) == {"value": "a", "other_value": "b", "extra": "c", "more_extra": "d"}


@db0.memo
class ObjectWithCustomLoad:
    __test__ = False
    
    def __init__(self, first, second, third):
        self.first = first
        self.second = second
        self.third = third        

    def __load__(self, **kwargs):
        """ Overloaded custom load only returns first attribute """
        return {
            "first": self.first,            
        }


def test_load_all_shallow(db0_fixture):
    obj = ObjectWithCustomLoad("one", "two", "three")
    loaded = db0.load_all(obj)
    # NOTE: load_all ignores custom __load__ method at the topmost level
    assert loaded == {"first": "one", "second": "two", "third": "three"}
    
    
def test_load_all_deep(db0_fixture):
    obj = ObjectWithCustomLoad("one", "two", ObjectWithCustomLoad("three", "four", "five"))
    loaded = db0.load_all(obj)
    # NOTE: load_all ignores custom __load__ method at the topmost level only
    assert loaded == {"first": "one", "second": "two", "third": {"first": "three"}}
    