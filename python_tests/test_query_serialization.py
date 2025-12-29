# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass


def test_serialized_query_can_be_stored_as_member(db0_fixture):
    objects = []
    for i in range(10):
        objects.append(MemoTestClass(i))
    db0.tags(*objects).add("tag1")    
    query_object = MemoTestClass(db0.find("tag1"))
    assert query_object is not None


def test_serialized_query_can_be_deserialized(db0_fixture, memo_tags):
    query_object = MemoTestClass(db0.find("tag1"))
    # run serialized query directly (it will be deserialized on the go)
    assert len(list(query_object.value)) == 10


def test_serde_tags_and_query(db0_fixture, memo_tags):
    query_object = MemoTestClass(db0.find("tag1", "tag2"))
    # run serialized query directly (it will be deserialized on the go)
    assert len(list(query_object.value)) == 5


def test_serde_tags_or_query(db0_fixture, memo_tags):
    query_object = MemoTestClass(db0.find(["tag2", "tag3"]))
    # run serialized query directly (it will be deserialized on the go)
    assert len(list(query_object.value)) == 7


def test_serde_tags_null_query(db0_fixture, memo_tags):
    # null query since tags2 is not present
    query_object = MemoTestClass(db0.find("tag_1", "tag2"))
    # run serialized query directly (it will be deserialized on the go)
    assert len(list(query_object.value)) == 0


def test_serde_tags_not_query(db0_fixture, memo_tags):
    # null query since tag_2 is not present
    query_object = MemoTestClass(db0.find("tag1", db0.no("tag2")))
    # run serialized query directly (it will be deserialized on the go)
    assert len(list(query_object.value)) == 5


def test_serde_range_query(db0_fixture):
    index = db0.index()
    for i in range(10):
        index.add(i, MemoTestClass(i))
    # null query since tag_2 is not present
    query_object = MemoTestClass(index.select(4, 8))
    # run serialized query directly (it will be deserialized on the go)
    assert len(list(query_object.value)) == 5


def test_serde_sort_query(db0_fixture):
    index = db0.index()
    for i in range(10):
        object = MemoTestClass(i)
        index.add(i, object)
        db0.tags(object).add("tag1")        
    # null query since tag_2 is not present
    query_object = MemoTestClass(index.sort(db0.find("tag1")))
    # run serialized query directly (it will be deserialized on the go)
    assert len(list(query_object.value)) == 10


def test_serialize_query_to_bytes(db0_fixture):
    objects = []
    for i in range(10):
        objects.append(MemoTestClass(i))
    db0.tags(*objects).add("tag1")
    bytes = db0.serialize(db0.find("tag1"))    
    assert bytes is not None


def test_deserialize_query_from_bytes(db0_fixture, memo_tags):
    bytes = db0.serialize(db0.find("tag1"))
    query = db0.deserialize(bytes)    
    assert len(list(query)) == 10


def test_deserialize_from_bytes_with_snapshot(db0_fixture, memo_tags):    
    db0.commit()
    snap = db0.snapshot()
    for i in range(5):
        db0.tags(MemoTestClass(i)).add("tag1")
    
    assert len(list(db0.find("tag1"))) == 15
    bytes = db0.serialize(db0.find("tag1"))
    # deserialize with snapshot
    snap_query = snap.deserialize(bytes)
    assert len(list(snap_query)) == 10


def test_serialize_sliced_query(db0_fixture, memo_tags):
    bytes = db0.serialize(db0.find("tag1")[2:])
    query = db0.deserialize(bytes)
    assert len(query) == len(db0.find("tag1")[2:])
