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


def test_uuid_can_be_generated_for_runnable(db0_fixture):
    objects = []
    for i in range(10):
        objects.append(MemoTestClass(i))
    db0.tags(*objects).add("tag1")
    uuid = db0.uuid(db0.find("tag1").as_runnable())    
    assert uuid is not None
