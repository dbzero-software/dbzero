from typing import Any
import pytest
import dbzero_ce as db0
import pickle
import io
from .memo_test_types import MemoTestClass
import json


def test_uid_of_memo_object(db0_fixture):
    object_1 = MemoTestClass(123)
    assert db0.uuid(object_1) is not None


def test_uid_has_base32_repr(db0_fixture):
    object_1 = MemoTestClass(123)
    uuid = db0.uuid(object_1)
    # only uppercase or digit characters
    assert all([c.isupper() or c.isdigit() for c in uuid])
    assert len(uuid) == 29


def test_uid_can_be_encoded_in_json(db0_fixture):
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
