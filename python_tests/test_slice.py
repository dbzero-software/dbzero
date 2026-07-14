# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass


def test_tag_query_results_can_be_sliced_left_bound(db0_no_autocommit, memo_tags):
    query = db0.find("tag1")
    all_items = [x for x in query]
    assert len(query[2:]) == len(query) - 2
    sliced_items = [x for x in query[2:]]
    assert all_items[2:] == sliced_items
    
    
def test_tag_query_results_can_be_sliced_right_bound(db0_no_autocommit, memo_tags):
    query = db0.find("tag1")
    all_items = [x.value for x in query]
    assert len(query[:-2]) == len(query) - 2
    sliced_items = [x.value for x in query[:-2]]
    assert all_items[:-2] == sliced_items
    
    
def test_tag_query_results_can_be_sliced_both_side_bound(db0_no_autocommit, memo_tags):
    query = db0.find("tag1")
    all_items = [x.value for x in query]
    sliced_items = [x.value for x in query[1:4]]
    assert all_items[1:4] == sliced_items
    
    
def test_tag_query_results_can_be_sliced_with_positive_step(db0_no_autocommit, memo_tags):
    query = db0.find("tag1")
    all_items = [x.value for x in query]
    sliced_items = [x.value for x in query[:8:2]]
    assert all_items[:8:2] == sliced_items
    
    
def test_slicing_already_scliced_queries(db0_no_autocommit, memo_tags):
    with pytest.raises(Exception):
        _ = db0.find("tag1")[1:4][:-2]


def test_default_slice_yields_identity(db0_no_autocommit, memo_tags):    
    query = db0.find("tag1")
    assert query[::] is query
    
    
def test_default_slice_of_already_sliced_query(db0_no_autocommit, memo_tags):
    sliced = db0.find("tag1")[:4]
    assert sliced[::] is sliced
    
    
def test_slicing_sorted_results(db0_no_autocommit, memo_tags):
    index = db0.index()
    for value in [0, 12, 4192, 33, 99, 313, 7, 99, 14, 28]:
        index.add(value, MemoTestClass(value))
    
    sliced = list(x.value for x in index.sort(index.select(), desc = True)[:5])
    assert sliced == [4192, 313, 99, 99, 33]