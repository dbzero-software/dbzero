import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestThreeParamsClass, MemoTestCustomLoadClass


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
    assert db0.load(t1) == {1, "string", 999}

def test_load_py_set(db0_fixture):
    t1 = {1, "string", 999}
    assert db0.load(t1) == {1, "string", 999}

def test_load_returns_none_not_as_string(db0_fixture):
    assert db0.load(None) == None
    t1 = db0.set([1, "string", None])
    assert db0.load(t1) == {1, "string", None}

def test_load_with_default_load_method(db0_fixture):
    memo = MemoTestCustomLoadClass("value_1", "value_2", "value_3")
    assert db0.load(memo) == {
            "v1": "value_1",
            "v2_v3": {"value_2": "value_3"}
        }