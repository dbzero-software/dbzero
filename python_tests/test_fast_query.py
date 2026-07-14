# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import KVTestClass, MemoTestClass, MemoDataPxClass, TriColor
from .conftest import DB0_DIR, DATA_PX
import time
from typing import List
import multiprocessing


def test_simple_group_by_query(db0_fixture):
    keys = ["one", "two", "three"]
    objects = []
    for i in range(10):
        objects.append(KVTestClass(keys[i % 3], i))
    
    db0.tags(*objects).add("tag1")
    # commit in order to make results available in snapshot
    db0.commit()
    groups = db0.group_by(lambda row: row.key, db0.find("tag1"))
    assert len(groups) == 3
    assert groups["one"] == 4
    assert groups["two"] == 3
    assert groups["three"] == 3
    

def test_delta_group_by_query(db0_fixture):
    keys = ["one", "two", "three"]
    objects = []
    for i in range(10):
        objects.append(KVTestClass(keys[i % 3], i))
    
    db0.tags(*objects).add("tag1")
    db0.commit()
    # first group by to feed the internal cache    
    groups = db0.group_by(lambda row: row.key, db0.find("tag1"))
    assert groups["one"] == 4
    db0.commit()
    
    # assign tags to 2 more objects
    db0.tags(KVTestClass("one", 11)).add("tag1")
    db0.tags(KVTestClass("three", 12)).add("tag1")
    db0.commit()
    
    # run as delta query
    groups = db0.group_by(lambda row: row.key, db0.find("tag1"))
    assert len(groups) == 3
    assert groups["one"] == 5
    assert groups["two"] == 3
    assert groups["three"] == 4


def test_delta_query_with_removals(db0_fixture):
    keys = ["one", "two", "three"]
    objects = []
    for i in range(10):
        objects.append(KVTestClass(keys[i % 3], i))
    
    db0.tags(*objects).add("tag1")
    db0.commit()
    # first group by to feed the internal cache
    db0.group_by(lambda row: row.key, db0.find("tag1"))
    
    # remove tags from 2 objects
    db0.tags(objects[1], objects[6]).remove("tag1")
    db0.commit()
    
    # run as delta query
    groups = db0.group_by(lambda row: row.key, db0.find("tag1"))    
    assert len(groups) == 3
    assert groups["one"] == 3
    assert groups["two"] == 2
    assert groups["three"] == 3


def test_delta_from_non_identical_queries(db0_fixture):
    keys = ["one", "two", "three"]
    objects = []
    for i in range(10):
        objects.append(KVTestClass(keys[i % 3], i))
    
    db0.tags(*objects).add("tag1")
    db0.tags(*objects).add("tag2")
    db0.tags(*objects).add("tag3")
    db0.commit()
    
    # first group by to feed the internal cache
    db0.group_by(lambda row: row.key, db0.find(["tag1", "tag2", "tag3"]))
    
    # assign tag3 to 2 more objects
    db0.tags(KVTestClass("one", 11)).add("tag4")
    db0.tags(KVTestClass("three", 12)).add("tag4")
    db0.commit()
    
    # run as delta query (but adding the additional optional tag)
    groups = db0.group_by(lambda row: row.key, db0.find(["tag1", "tag2", "tag3", "tag4"]))
    assert len(groups) == 3
    assert groups["one"] == 5
    assert groups["two"] == 3
    assert groups["three"] == 4


def test_group_by_enum_values(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    # commit to make results available in snapshot
    db0.commit()
    groups = db0.group_by(Colors.values(), db0.find(MemoTestClass))
    assert len(groups) == 3
    assert groups['RED'] == 4
    assert groups['GREEN'] == 3
    assert groups['BLUE'] == 3


def test_group_by_enum_values_with_tag_removals(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    db0.commit()    
    assert db0.group_by(Colors.values(), db0.find(MemoTestClass))["RED"] == 4
    i = 0
    for _ in range(4):
        db0.tags(next(iter(db0.find(MemoTestClass, Colors.RED)))).remove(Colors.RED)
        i += 1
        db0.commit()
        result = db0.group_by(Colors.values(), db0.find(MemoTestClass)).get("RED", None)
        count = result if result else 0
        assert count == 4 - i
    
    
def test_group_by_multiple_criteria(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    db0.commit()
    # group by all colors and then by even/odd values
    groups = db0.group_by((Colors.values(), lambda x: x.value % 2), db0.find(MemoTestClass))
    assert len(groups) == 6
    assert groups[("RED", 0)] == 2
    assert groups[("RED", 1)] == 2
    
    
def test_fast_query_with_separate_prefix_for_cache(db0_fixture, memo_scoped_enum_tags):
    db0.close()
    db0.init(DB0_DIR)
    db0.open(DATA_PX, "r")
    fq_prefix = "px-fast-query"
    db0.open(fq_prefix, "rw")
    db0.init_fast_query(fq_prefix)
    Colors = memo_scoped_enum_tags["Colors"]
    # first run to feed cache
    db0.group_by((Colors.values(), lambda x: "test"), db0.find(MemoDataPxClass))
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(DATA_PX, "r")
    db0.open(fq_prefix, "rw")
    
    # run again to use cache
    groups = db0.group_by((Colors.values(), lambda x: "test"), db0.find(MemoDataPxClass))
    assert type(groups) == dict


def test_group_by_with_custom_op(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    db0.commit()
    # group by all colors and then by even/odd values
    groups = db0.group_by((Colors.values(), lambda x: x.value % 2), db0.find(MemoTestClass), ops = (db0.make_sum(lambda x: x.value),))    
    assert sum(v for _, v in groups.items()) == 45
    
    
def test_group_by_with_multiple_ops(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    db0.commit()
    # group by all colors and then by even/odd values
    query_ops = (db0.count_op, db0.make_sum(lambda x: x.value))
    groups = db0.group_by((Colors.values(), lambda x: x.value % 2), db0.find(MemoTestClass), ops = query_ops)    
    assert sum(v[0] for _, v in groups.items()) == 10
    assert sum(v[1] for _, v in groups.items()) == 45
    
    
def test_group_by_with_multiple_ops_and_constant(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    db0.commit()
    # group by all colors and then by even/odd values
    query_ops = (db0.count_op, db0.make_sum(lambda x: x.value))
    groups = db0.group_by((Colors.values(), lambda x: "2024", lambda x: x.value % 2), db0.find(MemoTestClass), ops = query_ops)    
    for k in groups.keys():
        assert len(k) == 3


def create_process(num_objects: List, px_name):
    db0.init(DB0_DIR)
    db0.open(px_name.name, "rw")
    for count in num_objects:
        for _ in range(count):
            obj = MemoTestClass(0)
            db0.tags(obj).add("tag1")
        db0.commit()
        time.sleep(0.05)
    db0.close()
    

def test_refreshing_group_by_results(db0_fixture, memo_enum_tags):
    """
    In this test, one process is generating data while the other - running group_by queries.
    """
    px_name = db0.get_current_prefix()

    db0.close()
    
    num_objects = [5, 10, 11, 6, 22,8, 11, 6]    
    p = multiprocessing.Process(target=create_process, args = (num_objects, px_name))
    p.start()
    
    # start the reader process
    try:
        db0.init(DB0_DIR)
        db0.init_fast_query("__fq_cache/data")
        db0.open(px_name.name, "r")
        
        result = {0:0}
        while result and result[0] < sum(num_objects):
            # NOTE: we might call db0.refresh() but it's also performed automatically            
            result = db0.group_by(lambda x: x.value, db0.find(MemoTestClass, "tag1"))
            time.sleep(0.05)

    finally:
        p.terminate()
        p.join()
        db0.close()
    
    
def test_group_by_issue_1(db0_fixture, memo_enum_tags):
    db0.commit()    
    query_ops = (db0.count_op, db0.make_sum(lambda x: x.value))
    groups = db0.group_by(lambda x: "A" if (x.value % 2 == 0) else "B", db0.find(MemoTestClass), ops = query_ops)
    assert sum(v[1] for _, v in groups.items()) == 45
    
    
def test_group_by_enum_value_repr(db0_fixture):
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
    db0.init_fast_query("__fq_cache")
    db0.open("__fq_cache", "rw")
    db0.open(DATA_PX, "r")
    # change default prefix (where enum does not exist)
    db0.open(px_name, "r")
    
    # group by enum values-repr    
    result = db0.group_by(TriColor.values(), db0.find(MemoDataPxClass))    
    assert len(result) == 3
    
    
def test_group_by_queries_differing_by_group_criteria(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    db0.commit()
    # make sure the 2 queries are resolved as different ones
    groups_1 = db0.group_by((Colors.values(), lambda x: x.value % 2), db0.find(MemoTestClass))
    groups_2 = db0.group_by((Colors.values(), lambda x: x.value % 3), db0.find(MemoTestClass))
    assert len(groups_1) != len(groups_2)
    
    
def test_get_lambda_source(db0_fixture):
    def __call(func, **kwargs):        
        return db0.get_lambda_source(func)
    
    assert __call(lambda x: x.value) == "x.value"
    assert __call((lambda x:   x.value % 3), first = "first", second = "second") == "x.value % 3"