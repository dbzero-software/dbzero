import pytest
import dbzero_ce as db0
from .memo_test_types import KVTestClass


def test_simple_group_by_query(db0_fixture):
    keys = ["one", "two", "three"]
    objects = []
    for i in range(10):
        objects.append(KVTestClass(keys[i % 3], i))
    
    db0.tags(*objects).add("tag1")
    groups = db0.group_by(lambda row: row.key, db0.find("tag1"))
    assert len(groups) == 3
    assert groups["one"].count() == 4
    assert groups["two"].count() == 3
    assert groups["three"].count() == 3
    
    
def test_delta_group_by_query(db0_fixture):
    keys = ["one", "two", "three"]
    objects = []
    for i in range(10):
        objects.append(KVTestClass(keys[i % 3], i))
    
    db0.tags(*objects).add("tag1")
    # first group by to feed the internal cache
    db0.group_by(lambda row: row.key, db0.find("tag1"))    
    db0.commit()
    # assign tags to 2 more objects
    db0.tags(KVTestClass("one", 11)).add("tag1")
    db0.tags(KVTestClass("three", 12)).add("tag1")
        
    # run as delta query
    groups = db0.group_by(lambda row: row.key, db0.find("tag1"))
    assert len(groups) == 3
    assert groups["one"].count() == 5
    assert groups["two"].count() == 3
    assert groups["three"].count() == 4


def test_delta_query_with_removals(db0_fixture):
    keys = ["one", "two", "three"]
    objects = []
    for i in range(10):
        objects.append(KVTestClass(keys[i % 3], i))
    
    db0.tags(*objects).add("tag1")
    # first group by to feed the internal cache
    db0.group_by(lambda row: row.key, db0.find("tag1"))
    db0.commit()
    # remove tags from 2 objects
    db0.tags(objects[1], objects[6]).remove("tag1")

    # run as delta query
    groups = db0.group_by(lambda row: row.key, db0.find("tag1"))
    assert len(groups) == 3    
    assert groups["one"].count() == 3
    assert groups["two"].count() == 2
    assert groups["three"].count() == 3
