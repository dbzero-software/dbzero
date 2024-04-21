import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


# FIXME: failing test blocked
# def test_serialized_query_can_be_stored_as_member(db0_fixture):
#     objects = []
#     for i in range(10):
#         objects.append(MemoTestClass(i))
#     db0.tags(*objects).add("tag1")
#     print("Tags added")
#     query_object = MemoTestClass(db0.find("tag1"))
#     print("Query object created")