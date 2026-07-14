# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoDataPxClass, DATA_PX, TriColor
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


def test_enum_type_cannot_be_redefined(db0_fixture):
    Colors1 = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    # exception because type "Colors" is being redefined
    with pytest.raises(Exception):
        Colors = db0.enum("Colors", ["ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE", "TEN"])


def test_enum_values_order_is_preserved(db0_fixture):
    NewColors = db0.enum("NewColors", ["ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE", "TEN"])
    assert list(NewColors.values()) == [NewColors.ONE, NewColors.TWO, NewColors.THREE, NewColors.FOUR, NewColors.FIVE, 
                                        NewColors.SIX, NewColors.SEVEN, NewColors.EIGHT, NewColors.NINE, NewColors.TEN]
    

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


def test_enum_value_created_from_string(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])    
    Colors['RED'] is Colors.RED    
    Colors['GREEN'] is Colors.GREEN


def test_enum_value_created_from_int(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    assert Colors[0] is Colors.RED
    assert Colors[1] is Colors.GREEN
    assert Colors[2] is Colors.BLUE
    
    
def test_enum_value_repr_returned_from_enum_values_if_unable_to_create_enum(db0_fixture):
    px_name = db0.get_current_prefix().name
    db0.open(DATA_PX, "rw")
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(px_name, "r")    
    db0.open(DATA_PX, "r")
    db0.open("other-prefix", "rw")
    # try looking up in DATA_PX which is read-only
    db0.split_by(TriColor.values(), db0.find(MemoDataPxClass))
    

@db0.enum(values = ["RED", "GREEN", "BLUE"])
class ColorsEnum:
    pass


def func_to_test(color=ColorsEnum.RED):
    return color

    
def test_enum_value_as_default_param(db0_fixture):
    """
    This test assures that enum values can be used as default parameters in functions
    which is resovled as enum-value-repr before db0 is initialized
    """
    assert func_to_test() == ColorsEnum.RED
    assert func_to_test(ColorsEnum.GREEN) == ColorsEnum.GREEN
    assert func_to_test(ColorsEnum.BLUE) == ColorsEnum.BLUE
    
    
def test_enum_value_value_repr_compare(db0_fixture):
    """
    This tests run equal / non-equal comparison between enum values and their repr
    """
    red_repr = func_to_test()
    assert red_repr == ColorsEnum.RED
    assert ColorsEnum.RED == red_repr

    assert red_repr != ColorsEnum.GREEN
    assert ColorsEnum.GREEN != red_repr
    
    assert red_repr != "RED"


def test_enum_value_hash(db0_fixture):
    db0.hash((ColorsEnum.RED, ColorsEnum.GREEN)) == db0.hash((ColorsEnum.RED, ColorsEnum.GREEN))
    db0.hash(ColorsEnum.RED) != db0.hash(ColorsEnum.GREEN)
    
    
def test_enum_values_pulled_from_current_prefix(db0_fixture):
    val_1 = ColorsEnum.RED
    # change current prefix
    db0.open("some-other-prefix", "rw")
    val_2 = ColorsEnum.RED
    # the 2 values are from different prefixes
    assert db0.get_prefix_of(val_1) != db0.get_prefix_of(val_2)
    
    
def test_enum_values_from_different_prefixes_are_compared_equal(db0_fixture):
    val_1 = ColorsEnum.RED
    # change current prefix
    db0.open("some-other-prefix", "rw")
    val_2 = ColorsEnum.RED
    # make sure the 2 values from different prefixes are equal
    assert val_1 == val_2
    
    
def test_looking_up_by_enum_from_different_prefix(db0_fixture):
    set_1 = db0.set([ColorsEnum.RED])
    # change current prefix
    db0.open("some-other-prefix", "rw")
    # look up by enum value from different prefix
    assert ColorsEnum.RED in set_1


def test_using_db0_enum_as_python_dict_keys(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    d = {Colors.RED: "red", Colors.GREEN: "green", Colors.BLUE: "blue"}
    # change current prefix
    db0.open("some-other-prefix", "rw")
    States = db0.enum("States", ["TX", "CA", "NY"])
    state_tx = States.TX
    Countries = db0.enum("Countries", ["USA", "CAN", "MEX"])
    obj_1, obj_2, obj_3 = MemoTestClass(Colors.RED), MemoTestClass(Colors.GREEN), MemoTestClass(Colors.BLUE)
    assert db0.get_prefix_of(obj_1.value).name == "some-other-prefix"
    assert d[obj_1.value] == "red"
    assert d[obj_2.value] == "green"
    assert d[obj_3.value] == "blue"


def function_with_enum_as_default(color=ColorsEnum.RED):
    return MemoTestClass(color)

def test_enum_as_default_function_arguments(db0_fixture):    
    obj = function_with_enum_as_default()
    assert obj.value == ColorsEnum.RED


def test_enum_value_serialize(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    # this will serialize a concrete, materialized enum value
    bytes = db0.serialize(Colors.RED)
    assert len(bytes) > 0


def test_enum_value_deserialize(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    # this will serialize a concrete, materialized enum value
    bytes = db0.serialize(Colors.RED), db0.serialize(Colors.GREEN), db0.serialize(Colors.BLUE)
    assert db0.deserialize(bytes[0]) == Colors.RED
    assert db0.deserialize(bytes[1]) == Colors.GREEN
    assert db0.deserialize(bytes[2]) == Colors.BLUE
    

def get_repr_func(color=ColorsEnum.RED):
    # NOTE: this function by default returns the enum value repr (since the default argument is evaluated before db0 is initialized)
    return color
    
def test_enum_value_repr_serialize(db0_fixture):
    # this will serialize as enumvalue-repr
    bytes = db0.serialize(get_repr_func())
    assert len(bytes) > 0


def test_enum_value_repr_deserialize(db0_fixture):
    # this will serialize as enumvalue-repr
    bytes = db0.serialize(get_repr_func())
    red = ColorsEnum.RED
    assert db0.deserialize(bytes) == red
        

def test_enum_value_repr_deserialize_as_repr(db0_fixture):
    # this will serialize as enumvalue-repr
    bytes = db0.serialize(get_repr_func())
    # NOTICE: this will be deserialized as repr because enum value has not been created yet
    assert db0.deserialize(bytes) == ColorsEnum.RED
    
    
def test_is_enum_value(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    assert db0.is_enum_value(Colors.RED)
    assert db0.is_enum_value(TriColor.RED)
    assert db0.is_enum_value(get_repr_func())
    obj = MemoTestClass(Colors.RED)
    assert not db0.is_enum_value(obj)
    

def test_is_enum_value(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    assert db0.is_enum(Colors)
    assert db0.is_enum(TriColor)
    assert not db0.is_enum(MemoTestClass)


def get_repr_values_func(color=ColorsEnum):
    return color.values()
    
def test_enum_value_repr_destruct_issue_1(db0_fixture):
    expected_values = {"RED", "GREEN", "BLUE"}
    for _ in range(100):
        for value in get_repr_values_func():
            assert str(value) in expected_values


def test_deserialize_enum_value_from_unknown_prefix(db0_fixture):
    px_name = db0.get_current_prefix().name
    # create & open another prefix
    db0.open("alien-prefix", "rw")
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])        
    bytes = db0.serialize(Colors.RED)    
    db0.close()
    db0.init(DB0_DIR)
    # NOTE: we don't open the "alien-prefix" here
    db0.open(px_name, "r")
    assert db0.deserialize(bytes) == Colors.RED


def test_enum_value_repr_collection_lookup(db0_fixture):
    # colors created on current / default prefix
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])    
    db0.open("other-prefix", "rw")
    root = MemoTestSingleton(db0.set(), db0.dict())
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open("other-prefix", "r")
    root = db0.fetch(MemoTestSingleton)
    # look up by enum value repr
    assert Colors.values()[0] not in root.value
    assert Colors.values()[0] not in root.value_2


def test_enum_value_repr_is_hashablecollection_lookup(db0_fixture):
    # colors created on current / default prefix
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])    
    db0.open("other-prefix", "rw")
    root = MemoTestSingleton(db0.set(), db0.dict())
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open("other-prefix", "r")
    root = db0.fetch(MemoTestSingleton)
    # look up by enum value repr
    assert Colors.values()[0] not in root.value
    assert Colors.values()[0] not in root.value_2


def test_enum_value_repr_lookup_in_python_collection(db0_fixture):
    # colors created on current / default prefix
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])    
    db0.open("other-prefix", "rw")    
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open("other-prefix", "r")    
    # look up by enum value repr
    assert Colors.values()[0] not in set()
    assert Colors.values()[0] not in {}
