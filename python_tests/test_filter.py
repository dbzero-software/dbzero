# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass


def test_lambda_filter_applied_on_top_of_find(db0_fixture, memo_tags):
    query = db0.filter(lambda x: x.value == 1, db0.find("tag1"))
    assert len(list(query)) == 1
    
    
def test_split_filtered_query(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    query = db0.split_by(Colors.values(), db0.filter(lambda x: x.value > 2, db0.find(MemoTestClass)))
    counts = {}
    # split query returns an item + decorator tuples
    for _, decor in query:
        counts[decor] = counts.get(decor, 0) + 1
    
    assert counts[Colors.RED] == 3
    assert counts[Colors.GREEN] == 2
    assert counts[Colors.BLUE] == 2
    
    
def test_sort_filtered_query(db0_fixture, memo_tags):
    ix_value = db0.index()
    for obj in db0.find(MemoTestClass, "tag1"):
        ix_value.add(obj.value, obj)
        
    query = ix_value.sort(db0.filter(lambda x: x.value % 3 != 0, db0.find(MemoTestClass, "tag1")))
    sorted_values = [x.value for x in query]
    assert sorted_values == [1, 2, 4, 5, 7, 8]
    
    