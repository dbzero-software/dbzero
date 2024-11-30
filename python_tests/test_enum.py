import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


def test_create_enum_type(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    assert Colors is not None


def test_enum_value_can_be_retrieved_by_name(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    assert Colors.RED is not None
    assert Colors.GREEN is not None
    assert Colors.BLUE is not None


def test_enum_raises_if_trying_to_pull_non_existing_value(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    with pytest.raises(Exception):
        Colors.PURPLE


def test_enums_can_be_added_as_tags(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    db0.tags(MemoTestClass(1)).add(Colors.RED)


def test_same_values_from_different_enums_are_distinguished(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    Palette = db0.enum("Palette", ["RED", "GREEN", "BLUE"])
    db0.tags(MemoTestClass(1)).add(Colors.RED)
    db0.tags(MemoTestClass(2)).add(Palette.RED)
    assert set([x.value for x in db0.find(Colors.RED)]) == set([1])
    assert set([x.value for x in db0.find(Palette.RED)]) == set([2])


def test_enum_tags_are_distinguished_from_string_values(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])    
    db0.tags(MemoTestClass(1)).add(Colors.RED)
    db0.tags(MemoTestClass(2)).add("RED")
    assert set([x.value for x in db0.find("RED")]) == set([2])
    assert set([x.value for x in db0.find(Colors.RED)]) == set([1])


def test_enum_type_defines_values_method(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    assert len(Colors.values()) == 3
    assert Colors.RED in Colors.values()
    assert Colors.GREEN in Colors.values()
    assert Colors.BLUE in Colors.values()
    
    
def test_enum_values_can_be_stored_as_members(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    obj_1 = MemoTestClass(Colors.RED)
    obj_2 = MemoTestClass(Colors.GREEN)
    assert obj_1.value == Colors.RED
    assert obj_2.value == Colors.GREEN
    
    
def test_enum_values_can_be_stored_as_dict_keys(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    dict = db0.dict({Colors.RED: "red", Colors.GREEN: "green"})
    _ = MemoTestSingleton(dict)
    assert dict[Colors.RED] == "red"
    assert dict[Colors.GREEN] == "green"


def test_enums_tags_can_be_removed(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    obj_1 = MemoTestClass(1)
    obj_2 = MemoTestClass(2)
    db0.tags(obj_1).add([Colors.RED, Colors.BLUE])
    db0.tags(obj_2).add([Colors.BLUE, Colors.GREEN])
    assert len(list(db0.find(Colors.BLUE))) == 2
    db0.tags(obj_1).remove(Colors.BLUE)
    assert len(list(db0.find(Colors.BLUE))) == 1
    assert len(list(db0.find(Colors.RED))) == 1
    db0.tags(obj_2).remove(Colors.BLUE)
    assert len(list(db0.find(Colors.BLUE))) == 0


def test_enum_value_str_conversion(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    assert str(Colors.RED) == "RED"


def test_enum_value_repr(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    assert repr(Colors.RED) == "<EnumValue Colors.RED>"


def test_enum_values_order_is_preserved(db0_fixture):
    Colors = db0.enum("Colors", ["ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE", "TEN"])
    assert list(Colors.values()) == [Colors.ONE, Colors.TWO, Colors.THREE, Colors.FOUR, Colors.FIVE, Colors.SIX,
        Colors.SEVEN, Colors.EIGHT, Colors.NINE, Colors.TEN]
    

def test_load_enum_value(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    str_repr = ["RED", "GREEN", "BLUE"]
    for index, val in enumerate(Colors.values()):
        assert db0.load(val) == str_repr[index]


def test_enum_value_repr_returned_if_unable_to_create_enum(db0_fixture):
    # colors created on current / default prefix
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    db0.open("other-prefix", "rw")
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open("other-prefix", "r")
    # attempt retrieving colors from "other_prefix" (read-only)
    values = Colors.values()
    assert "???" in f"{values}"
    # FIXME: segfault when trying to access type
    # print(type(value))
