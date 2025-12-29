# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from typing import Any
import pytest
import dbzero as db0
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
    assert len(uuid) <= 24


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

        
def test_uuid_issue_1(db0_fixture):
    """
    Issue: https://github.com/wskozlowski/dbzero/issues/171
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
    