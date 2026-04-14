# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0


@db0.enum(values=["RED", "GREEN", "BLUE"])
class ColorsEnum:
    pass


@db0.enum(values=["RED", "GREEN", "BLUE"])
class PaletteEnum:
    pass


# capture EnumValueRepr before db0 is initialized (default param trick)
def _get_color_repr(color=ColorsEnum.RED):
    return color

def _get_color_green_repr(color=ColorsEnum.GREEN):
    return color

def _get_palette_repr(color=PaletteEnum.RED):
    return color


# ---- EnumValue comparisons across different prefixes ----

def test_enum_value_equal_across_prefixes(db0_fixture):
    val_1 = ColorsEnum.RED
    db0.open("other-prefix", "rw")
    val_2 = ColorsEnum.RED
    assert db0.get_prefix_of(val_1) != db0.get_prefix_of(val_2)
    assert val_1 == val_2
    assert not (val_1 != val_2)


def test_enum_value_not_equal_different_value_across_prefixes(db0_fixture):
    val_1 = ColorsEnum.RED
    db0.open("other-prefix", "rw")
    val_2 = ColorsEnum.GREEN
    assert db0.get_prefix_of(val_1) != db0.get_prefix_of(val_2)
    assert val_1 != val_2
    assert not (val_1 == val_2)


# ---- EnumValueRepr comparisons across different prefixes ----

def test_enum_value_repr_equal_across_prefixes(db0_fixture):
    """EnumValueRepr for the same enum and value should be equal regardless of prefix."""
    repr_1 = _get_color_repr()
    db0.open("other-prefix", "rw")
    repr_2 = _get_color_repr()
    assert repr_1 == repr_2
    assert not (repr_1 != repr_2)


def test_enum_value_repr_not_equal_different_value(db0_fixture):
    repr_red = _get_color_repr()
    repr_green = _get_color_green_repr()
    assert repr_red != repr_green
    assert not (repr_red == repr_green)


def test_enum_value_repr_not_equal_different_enum(db0_fixture):
    """Same value name but different enum types should not be equal."""
    repr_color = _get_color_repr()
    repr_palette = _get_palette_repr()
    assert repr_color != repr_palette
    assert not (repr_color == repr_palette)


# ---- EnumValue vs EnumValueRepr on the same prefix ----

def test_enum_value_equals_enum_value_repr_same_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    repr_val = _get_color_repr()
    assert enum_val == repr_val
    assert repr_val == enum_val
    assert not (enum_val != repr_val)
    assert not (repr_val != enum_val)


def test_enum_value_not_equal_enum_value_repr_different_value_same_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    repr_val = _get_color_green_repr()
    assert enum_val != repr_val
    assert repr_val != enum_val
    assert not (enum_val == repr_val)
    assert not (repr_val == enum_val)


# ---- EnumValue vs EnumValueRepr on different prefixes ----

def test_enum_value_equals_enum_value_repr_different_prefix(db0_fixture):
    repr_val = _get_color_repr()
    db0.open("other-prefix", "rw")
    enum_val = ColorsEnum.RED
    assert db0.get_prefix_of(enum_val).name == "other-prefix"
    assert enum_val == repr_val
    assert repr_val == enum_val
    assert not (enum_val != repr_val)
    assert not (repr_val != enum_val)


def test_enum_value_not_equal_enum_value_repr_different_value_different_prefix(db0_fixture):
    repr_val = _get_color_green_repr()
    db0.open("other-prefix", "rw")
    enum_val = ColorsEnum.RED
    assert db0.get_prefix_of(enum_val).name == "other-prefix"
    assert enum_val != repr_val
    assert repr_val != enum_val
    assert not (enum_val == repr_val)
    assert not (repr_val == enum_val)


# ---- set/in lookup with EnumValue across different prefixes ----

def test_in_set_enum_value_across_prefixes(db0_fixture):
    val_1 = ColorsEnum.RED
    s = {val_1}
    db0.open("other-prefix", "rw")
    val_2 = ColorsEnum.RED
    assert db0.get_prefix_of(val_1) != db0.get_prefix_of(val_2)
    assert val_2 in s


def test_in_set_enum_value_different_value_across_prefixes(db0_fixture):
    val_1 = ColorsEnum.RED
    s = {val_1}
    db0.open("other-prefix", "rw")
    val_2 = ColorsEnum.GREEN
    assert val_2 not in s


# ---- set/in lookup with EnumValueRepr across different prefixes ----

def test_in_set_enum_value_repr_across_prefixes(db0_fixture):
    repr_1 = _get_color_repr()
    s = {repr_1}
    db0.open("other-prefix", "rw")
    repr_2 = _get_color_repr()
    assert repr_2 in s


def test_in_set_enum_value_repr_different_value(db0_fixture):
    repr_red = _get_color_repr()
    s = {repr_red}
    repr_green = _get_color_green_repr()
    assert repr_green not in s


# ---- set/in lookup between EnumValue and EnumValueRepr on same prefix ----

def test_in_set_enum_value_by_repr_same_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    s = {enum_val}
    repr_val = _get_color_repr()
    assert repr_val in s


def test_in_set_enum_repr_by_value_same_prefix(db0_fixture):
    repr_val = _get_color_repr()
    s = {repr_val}
    enum_val = ColorsEnum.RED
    assert enum_val in s


def test_in_set_enum_value_by_repr_different_value_same_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    s = {enum_val}
    repr_green = _get_color_green_repr()
    assert repr_green not in s


# ---- set/in lookup between EnumValue and EnumValueRepr on different prefixes ----

def test_in_set_enum_value_by_repr_different_prefix(db0_fixture):
    repr_val = _get_color_repr()
    s = {repr_val}
    db0.open("other-prefix", "rw")
    enum_val = ColorsEnum.RED
    assert db0.get_prefix_of(enum_val).name == "other-prefix"
    assert enum_val in s


def test_in_set_enum_repr_by_value_different_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    s = {enum_val}
    db0.open("other-prefix", "rw")
    repr_val = _get_color_repr()
    assert repr_val in s


def test_in_set_enum_value_by_repr_different_value_different_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    s = {enum_val}
    db0.open("other-prefix", "rw")
    repr_green = _get_color_green_repr()
    assert repr_green not in s


# ---- dict lookup with EnumValue across different prefixes ----

def test_in_dict_enum_value_across_prefixes(db0_fixture):
    val_1 = ColorsEnum.RED
    d = {val_1: "red"}
    db0.open("other-prefix", "rw")
    val_2 = ColorsEnum.RED
    assert db0.get_prefix_of(val_1) != db0.get_prefix_of(val_2)
    assert val_2 in d
    assert d[val_2] == "red"


def test_in_dict_enum_value_different_value_across_prefixes(db0_fixture):
    val_1 = ColorsEnum.RED
    d = {val_1: "red"}
    db0.open("other-prefix", "rw")
    val_2 = ColorsEnum.GREEN
    assert val_2 not in d


# ---- dict lookup with EnumValueRepr across different prefixes ----

def test_in_dict_enum_value_repr_across_prefixes(db0_fixture):
    repr_1 = _get_color_repr()
    d = {repr_1: "red"}
    db0.open("other-prefix", "rw")
    repr_2 = _get_color_repr()
    assert repr_2 in d
    assert d[repr_2] == "red"


def test_in_dict_enum_value_repr_different_value(db0_fixture):
    repr_red = _get_color_repr()
    d = {repr_red: "red"}
    repr_green = _get_color_green_repr()
    assert repr_green not in d


# ---- dict lookup between EnumValue and EnumValueRepr on same prefix ----

def test_in_dict_enum_value_by_repr_same_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    d = {enum_val: "red"}
    repr_val = _get_color_repr()
    assert repr_val in d
    assert d[repr_val] == "red"


def test_in_dict_enum_repr_by_value_same_prefix(db0_fixture):
    repr_val = _get_color_repr()
    d = {repr_val: "red"}
    enum_val = ColorsEnum.RED
    assert enum_val in d
    assert d[enum_val] == "red"


def test_in_dict_enum_value_by_repr_different_value_same_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    d = {enum_val: "red"}
    repr_green = _get_color_green_repr()
    assert repr_green not in d


# ---- dict lookup between EnumValue and EnumValueRepr on different prefixes ----

def test_in_dict_enum_value_by_repr_different_prefix(db0_fixture):
    repr_val = _get_color_repr()
    d = {repr_val: "red"}
    db0.open("other-prefix", "rw")
    enum_val = ColorsEnum.RED
    assert db0.get_prefix_of(enum_val).name == "other-prefix"
    assert enum_val in d
    assert d[enum_val] == "red"


def test_in_dict_enum_repr_by_value_different_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    d = {enum_val: "red"}
    db0.open("other-prefix", "rw")
    repr_val = _get_color_repr()
    assert repr_val in d
    assert d[repr_val] == "red"


def test_in_dict_enum_value_by_repr_different_value_different_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    d = {enum_val: "red"}
    db0.open("other-prefix", "rw")
    repr_green = _get_color_green_repr()
    assert repr_green not in d


# ---- list/in lookup with EnumValue across different prefixes ----

def test_in_list_enum_value_across_prefixes(db0_fixture):
    val_1 = ColorsEnum.RED
    lst = [val_1]
    db0.open("other-prefix", "rw")
    val_2 = ColorsEnum.RED
    assert db0.get_prefix_of(val_1) != db0.get_prefix_of(val_2)
    assert val_2 in lst


def test_in_list_enum_value_different_value_across_prefixes(db0_fixture):
    val_1 = ColorsEnum.RED
    lst = [val_1]
    db0.open("other-prefix", "rw")
    val_2 = ColorsEnum.GREEN
    assert val_2 not in lst


# ---- list/in lookup with EnumValueRepr across different prefixes ----

def test_in_list_enum_value_repr_across_prefixes(db0_fixture):
    repr_1 = _get_color_repr()
    lst = [repr_1]
    db0.open("other-prefix", "rw")
    repr_2 = _get_color_repr()
    assert repr_2 in lst


def test_in_list_enum_value_repr_different_value(db0_fixture):
    repr_red = _get_color_repr()
    lst = [repr_red]
    repr_green = _get_color_green_repr()
    assert repr_green not in lst


# ---- list/in lookup between EnumValue and EnumValueRepr on same prefix ----

def test_in_list_enum_value_by_repr_same_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    lst = [enum_val]
    repr_val = _get_color_repr()
    assert repr_val in lst


def test_in_list_enum_repr_by_value_same_prefix(db0_fixture):
    repr_val = _get_color_repr()
    lst = [repr_val]
    enum_val = ColorsEnum.RED
    assert enum_val in lst


def test_in_list_enum_value_by_repr_different_value_same_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    lst = [enum_val]
    repr_green = _get_color_green_repr()
    assert repr_green not in lst


# ---- list/in lookup between EnumValue and EnumValueRepr on different prefixes ----

def test_in_list_enum_value_by_repr_different_prefix(db0_fixture):
    repr_val = _get_color_repr()
    lst = [repr_val]
    db0.open("other-prefix", "rw")
    enum_val = ColorsEnum.RED
    assert db0.get_prefix_of(enum_val).name == "other-prefix"
    assert enum_val in lst


def test_in_list_enum_repr_by_value_different_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    lst = [enum_val]
    db0.open("other-prefix", "rw")
    repr_val = _get_color_repr()
    assert repr_val in lst


def test_in_list_enum_value_by_repr_different_value_different_prefix(db0_fixture):
    enum_val = ColorsEnum.RED
    lst = [enum_val]
    db0.open("other-prefix", "rw")
    repr_green = _get_color_green_repr()
    assert repr_green not in lst
