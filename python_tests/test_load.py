import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton


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
    