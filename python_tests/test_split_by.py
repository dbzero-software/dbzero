import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


def test_query_can_be_split_by_list_of_tags(db0_fixture, memo_tags):
    # split_by returns a tuple of a quey and the associated observer
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
