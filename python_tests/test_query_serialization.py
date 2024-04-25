import pytest
import dbzero_ce as db0
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
