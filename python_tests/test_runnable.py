import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


def test_query_can_converted_to_runnable(db0_fixture):
    objects = []
    for i in range(10):
        objects.append(MemoTestClass(i))
    db0.tags(*objects).add("tag1")
    runnable = db0.find("tag1").as_runnable()
    assert runnable is not None


def test_runnable_with_multi_tag_query(db0_fixture):
    objects = []
    for i in range(10):
        objects.append(MemoTestClass(i))
    db0.tags(*objects).add("tag1", "tag2")
    runnable = db0.find(["tag1", "tag2"]).as_runnable()
    assert runnable is not None
    
            
def test_uuid_can_be_generated_for_runnable(db0_fixture):
    objects = []
    for i in range(10):
        objects.append(MemoTestClass(i))
    db0.tags(*objects).add("tag1")
    uuid = db0.uuid(db0.find("tag1").as_runnable())    
    assert uuid is not None


def test_runnable_uuid_is_same_even_with_differing_query_arameters(db0_fixture):
    objects = []
    for i in range(10):
        objects.append(MemoTestClass(i))
    db0.tags(*objects).add("tag1", "tag2")    
    uuid_1 = db0.uuid(db0.find(["tag1"]).as_runnable())    
    uuid_2 = db0.uuid(db0.find(["tag1", "tag2"]).as_runnable())    
    assert uuid_1 == uuid_2
    