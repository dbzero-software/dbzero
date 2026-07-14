# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import operator
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoClassForTags
from .conftest import DB0_DIR, DATA_PX
import datetime


@db0.memo
class MemoDate:
    def __init__(self, date: datetime.date):
        self.__date = date
    
    
def test_join_by_one_tag_type(db0_fixture):
    # 1. tag 2 objects of different types with the same tag
    obj1 = MemoTestClass("obj1")
    obj2 = MemoClassForTags("obj2")
    date = MemoDate(datetime.date.today())
    db0.tags(obj1, obj2).add(date)
    
    # 2. join the 2 collections by the common tag
    join_result = db0.join(db0.find(MemoTestClass), db0.find(MemoClassForTags), on = db0.find(MemoDate))
    assert list(join_result) == [(obj1, obj2)]


def test_join_simplified_syntax(db0_fixture):
    # 1. tag 2 objects of different types with the same tag
    obj1 = MemoTestClass("obj1")
    obj2 = MemoClassForTags("obj2")
    date = MemoDate(datetime.date.today())
    db0.tags(obj1, obj2).add(date)
    
    # 2. join the 2 collections by the common tag
    # NOTE: instead of db0.find we simply put type names to make it concise
    join_result = db0.join(MemoTestClass, MemoClassForTags, on = MemoDate)
    assert list(join_result) == [(obj1, obj2)]
