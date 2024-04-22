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


# def test_serialized_query_can_be_deserialized(db0_fixture):
#     objects = []
#     for i in range(10):
#         objects.append(MemoTestClass(i))
#     db0.tags(*objects).add("tag1")
#     query_object = MemoTestClass(db0.find("tag1"))
#     # run serialized query directly (it will be deserialized on the go)
#     assert len(query_object.value) == 10
