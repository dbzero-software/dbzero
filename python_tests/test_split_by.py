# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoDataPxClass, TriColor
from .conftest import DB0_DIR, DATA_PX


def test_query_can_be_split_by_list_of_tags(db0_fixture, memo_tags):
    # split_by returns a tuple of a query and the associated observer
    query = db0.split_by(["tag1", "tag2", "tag3", "tag4"], db0.find(MemoTestClass))
    assert len(list(query)) == 10


def test_split_by_adds_item_decorators(db0_fixture, memo_excl_tags):
    query = db0.split_by(["tag1", "tag2", "tag3", "tag4"], db0.find(MemoTestClass))
    counts = {}
    values = []
    # split query returns an item + decorator tuples
    for item, decor in query:        
        counts[decor] = counts.get(decor, 0) + 1
        values.append(item.value)
    
    assert counts["tag1"] == 3
    assert counts["tag2"] == 3
    assert counts["tag3"] == 2
    assert counts["tag4"] == 2
    assert set(values) == set([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])


def test_split_by_enum_values(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    query = db0.split_by(Colors.values(), db0.find(MemoTestClass))    
    counts = {}
    # split query returns an item + decorator tuples
    for _, decor in query:        
        counts[decor] = counts.get(decor, 0) + 1
    
    assert counts[Colors.RED] == 4
    assert counts[Colors.GREEN] == 3
    assert counts[Colors.BLUE] == 3


def test_split_query_can_be_used_as_subquery(db0_fixture, memo_tags):
    split_query = db0.split_by(["tag3", "tag4"], db0.find("tag1"))
    # AND-joined query retains all decorators (from inner split query)
    query = db0.find(split_query, "tag2")
    counts = {}
    values = []
    for item, decor in query:        
        counts[decor] = counts.get(decor, 0) + 1
        values.append(item.value)
    # since split_by is exclusive #0 may yield non-deterministic result since its tagged with both tag3 and tag4
    assert counts == {'tag3': 2, 'tag4': 2} or counts == {'tag3': 1, 'tag4': 3}
    assert set(values) == set([0, 4, 6, 8])
    
    
def test_split_by_enum_values_repr(db0_fixture):
    px_name = db0.get_current_prefix().name
    db0.open(DATA_PX, "rw")
    # enum if created on the data prefix
    colors = TriColor.values()
    # create scoped classes on data prefix
    for i in range(10):
        obj = MemoDataPxClass(i)
        db0.tags(obj).add(colors[i % 3])        
    del colors
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(DATA_PX, "r")
    # change default prefix (where enum does not exist)
    db0.open(px_name, "r")
    
    # split by enum values-repr
    # which should be converted to materialized enum values
    query = db0.split_by(TriColor.values(), db0.find(MemoDataPxClass))
    count = 0
    for _, v in query:
        # this is to distinguish enumvalue repr
        assert "?" not in repr(v)
        count += 1
    
    assert count == 10
    

def test_non_exclusive_split(db0_fixture):
    obj = MemoTestClass(0)
    db0.tags(obj).add(["tag1", "tag2"])
    # default split_by is exclusive
    assert len(list(db0.split_by(["tag1", "tag2"], db0.find(MemoTestClass)))) == 1
    assert len(list(db0.split_by(["tag1", "tag2"], db0.find(MemoTestClass), exclusive = True))) == 1
    assert len(list(db0.split_by(["tag1", "tag2"], db0.find(MemoTestClass), exclusive = False))) == 2
    