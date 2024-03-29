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
    assert len(uuid) == 32


def test_uid_can_be_encoded_in_json(db0_fixture):
    object_1 = MemoTestClass(123)
    uuid = db0.uuid(object_1)
    js_data = json.dumps({"uuid": uuid})    
    # decode from json
    data = json.loads(js_data)
    assert data["uuid"] == uuid
    