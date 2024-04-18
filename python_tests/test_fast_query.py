import pytest
import dbzero_ce as db0
from .memo_test_types import KVTestClass

    
# def test_simple_group_by(db0_fixture):
#     keys = ["one", "two", "three"]
#     objects = []
#     for i in range(10):
#         objects.append(KVTestClass(keys[i % 3], i))
    
#     db0.tags(*objects).add("tag1")
#     groups = db0.group_by(db0.find("tag1"))
#     print(groups)
