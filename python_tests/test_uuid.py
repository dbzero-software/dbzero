from typing import Any
import pytest
import dbzero_ce as db0
import pickle
import io
from .memo_test_types import MemoTestClass
import json


@db0.memo
class MemoWithInvalidUUID:
    def __init__(self):
        self.my_uuid = db0.uuid(self)


@db0.memo
class MemoWithUUID:
    def __init__(self):
        self.my_uuid = db0.uuid(db0.materialized(self))

    
def test_uuid_of_memo_object(db0_fixture):
    object_1 = MemoTestClass(123)
    assert db0.uuid(object_1) is not None


def test_uuid_has_base32_repr(db0_fixture):
    object_1 = MemoTestClass(123)
    uuid = db0.uuid(object_1)
    # only uppercase or digit characters
    assert all([c.isupper() or c.isdigit() for c in uuid])
    assert len(uuid) <= 22


def test_uuid_can_be_encoded_in_json(db0_fixture):
    object_1 = MemoTestClass(123)
    uuid = db0.uuid(object_1)
    js_data = json.dumps({"uuid": uuid})
    # decode from json
    data = json.loads(js_data)
    assert data["uuid"] == uuid


def test_uuid_can_be_generated_for_query_object(db0_fixture):
    objects = []
    for i in range(10):
        objects.append(MemoTestClass(i))
    db0.tags(*objects).add("tag1")
    query = db0.find("tag1")
    assert db0.uuid(query) is not None
    assert len(db0.uuid(query)) > 40


def test_query_uuid_is_same_between_transactions(db0_fixture):
    objects = []
    for i in range(10):
        objects.append(MemoTestClass(i))
    db0.tags(*objects).add("tag1")
    uuid1 = db0.uuid(db0.find("tag1"))
    assert uuid1 == db0.uuid(db0.find("tag1"))
    db0.commit()
    db0.tags(MemoTestClass(11)).add("tag1")
    uuid2 = db0.uuid(db0.find("tag1"))
    assert uuid1 == uuid2


def test_uuid_of_dict(db0_fixture):
    object_1 = db0.dict({"abd": 1})
    print(db0.uuid(object_1))
    assert db0.uuid(object_1) is not None


def test_fetch_dict_by_uuid(db0_fixture):
    object_1 = db0.dict({"abd": 1})    
    object_2 = db0.fetch(db0.uuid(object_1))
    assert object_1 == object_2


def test_uuid_of_set(db0_fixture):
    object_1 = db0.set([1, 2, 3])
    assert db0.uuid(object_1) is not None


def test_fetch_set_by_uuid(db0_fixture):
    object_1 = db0.set([1, 2, 3])
    object_2 = db0.fetch(db0.uuid(object_1))
    assert object_1 == object_2


def test_uuid_of_tuple(db0_fixture):
    object_1 = db0.tuple([1, 2, 3])
    assert db0.uuid(object_1) is not None


def test_fetch_tuple_by_uuid(db0_fixture):
    object_1 = db0.tuple([1, 2, 3])
    object_2 = db0.fetch(db0.uuid(object_1))
    assert object_1 == object_2
    
    
def test_uuid_issue_1(db0_fixture):
    """
    Issue: https://github.com/wskozlowski/dbzero_ce/issues/171
    Resolution: added validation and exception "Cannot get UUID of an uninitialized object"
    """
    with pytest.raises(Exception) as excinfo:
        _ = MemoWithInvalidUUID()
        assert "UUID" in str(excinfo.value)
    
    obj_1 = MemoWithUUID()
    obj_2 = MemoWithUUID()
    assert obj_1.my_uuid != obj_2.my_uuid
    assert db0.uuid(obj_1) == obj_1.my_uuid
    assert db0.uuid(obj_2) == obj_2.my_uuid
    