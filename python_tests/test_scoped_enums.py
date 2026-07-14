# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass


@db0.memo(prefix="scoped-class-prefix")
class ScopedDataClass:
    def __init__(self, value):
        self.value = value

    
@db0.enum(values=["RED", "GREEN", "BLACK"], prefix="scoped-class-prefix")
class ScopedColor:
    pass


@db0.enum(values=["RED", "GREEN", "BLACK"], prefix=None)
class XColor:
    pass

    
def test_enum_with_null_prefix_is_no_scoped(db0_fixture):
    obj = MemoTestClass(42)
    db0.tags(obj).add(XColor.RED)
    assert len(list(db0.find(MemoTestClass, XColor.RED))) == 1


def test_scoped_class_can_be_tagged_with_scoped_enum(db0_fixture):
    obj = ScopedDataClass(42)
    db0.tags(obj).add(ScopedColor.RED)
    assert len(list(db0.find(ScopedDataClass, ScopedColor.RED))) == 1


def test_scoped_enum_after_close(db0_fixture):
    obj = ScopedDataClass(42)
    db0.tags(obj).add(ScopedColor.RED)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    # open enum associated prefix for read-only
    db0.open("scoped-class-prefix", "r")
    assert len(list(db0.find(ScopedDataClass, ScopedColor.RED))) == 1


def test_get_prefix_of_works_for_enums(db0_fixture):
    assert db0.get_prefix_of(ScopedColor) is not None


def test_scoped_enum_values(db0_fixture):
    px_name = db0.get_current_prefix().name
    db0.open(db0.get_prefix_of(ScopedColor).name, "rw")
    # change current prefix
    db0.open(px_name)
    assert db0.get_prefix_of(ScopedColor.RED) is not None
    assert db0.get_prefix_of(ScopedColor.RED) != db0.get_current_prefix()
    
    
def test_scoped_enum_values_are_translated_across_prefixes(db0_fixture):
    px_name = db0.get_current_prefix().name
    db0.open(db0.get_prefix_of(ScopedColor).name, "rw")
    # change current prefix
    db0.open(px_name)
    obj = MemoTestClass(ScopedColor.RED)
    assert db0.get_prefix_of(ScopedColor.RED) != db0.get_prefix_of(obj)
    assert db0.get_prefix_of(obj.value) == db0.get_prefix_of(obj)
