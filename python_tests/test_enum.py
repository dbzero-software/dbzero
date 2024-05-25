import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


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
    