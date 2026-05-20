# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from random import random
import pytest
import dbzero as db0
from dataclasses import dataclass
from .conftest import DB0_DIR
import random
import gc


@db0.memo(immutable=True, no_default_tags=True)
@dataclass
class MemoImmutableClass1:
    data: str
    value: int = 0


@db0.memo(immutable=True, no_default_tags=True)
@dataclass
class MemoImmutableBytesClass:
    data: bytes


@db0.memo(immutable=True, no_default_tags=True)
@dataclass
class MemoImmutableLargePayloadClass:
    data: object


@db0.memo(immutable=True, no_default_tags=True)
@dataclass
class MemoImmutableNestedPayload:
    name: str
    count: int


@db0.memo(immutable=True, no_default_tags=True)
class MemoImmutableNestedHolder:
    def __init__(self, name, count, label):
        self.nested = MemoImmutableNestedPayload(name=name, count=count)
        self.label = label


@db0.memo(immutable=True, no_default_tags=True)
class MemoImmutablePreboundNestedHolder:
    def __init__(self, nested, label):
        self.nested = nested
        self.label = label


@db0.memo(immutable=True, no_default_tags=True)
class MemoImmutableReadInConstructor:
    def __init__(self, data, payload):
        self.data = data
        self.payload = payload
        self.seen_data = self.data
        self.seen_payload = self.payload
    
def test_create_memo_immutable(db0_fixture):
    _ = MemoImmutableClass1(data="immutable data", value=42)


def test_tag_and_find_immutable_instance(db0_fixture):
    obj_1 = MemoImmutableClass1(data="immutable data", value=42)
    db0.tags(obj_1).add("tag1", "tag2")
    assert list(db0.find("tag1")) == [obj_1]


def test_read_embedded_immutable_string_after_reopen(db0_fixture):
    obj = MemoImmutableClass1(data="small embedded string", value=7)
    db0.tags(obj).add("keep-embedded-string")
    obj_id = db0.uuid(obj)
    assert obj.data == "small embedded string"
    assert db0.fetch(obj_id).data == "small embedded string"

    del obj
    gc.collect()
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened = db0.fetch(obj_id)
    assert reopened.data == "small embedded string"
    assert reopened.value == 7
    del reopened
    gc.collect()


def test_read_embedded_immutable_bytes(db0_fixture):
    payload = b"a\x00b embedded bytes"
    obj = MemoImmutableBytesClass(payload)
    assert obj.data == payload


def test_large_immutable_string_and_bytes_fallback_read(db0_fixture):
    large_text = "x" * (12 * 1024)
    large_bytes = b"y" * (12 * 1024)

    text_obj = MemoImmutableLargePayloadClass(large_text)
    bytes_obj = MemoImmutableLargePayloadClass(large_bytes)

    assert text_obj.data == large_text
    assert bytes_obj.data == large_bytes


def test_read_embedded_immutable_values_inside_constructor(db0_fixture):
    payload = b"constructor\x00bytes"
    obj = MemoImmutableReadInConstructor("constructor string", payload)

    assert obj.seen_data == "constructor string"
    assert obj.seen_payload == payload
    assert obj.data == "constructor string"
    assert obj.payload == payload


def test_read_embedded_immutable_nested_object_after_reopen(db0_fixture):
    obj = MemoImmutableNestedHolder(name="embedded child", count=5, label="root")
    db0.tags(obj).add("keep-embedded-nested")
    obj_id = db0.uuid(obj)

    assert obj.nested.name == "embedded child"
    assert obj.nested.count == 5

    del obj
    gc.collect()
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened = db0.fetch(obj_id)
    assert reopened.nested.name == "embedded child"
    assert reopened.nested.count == 5
    assert isinstance(reopened.nested, MemoImmutableNestedPayload)


def test_prebound_immutable_nested_object_embeds_into_owner(db0_fixture):
    inner = MemoImmutableNestedPayload(name="prebound child", count=8)
    obj = MemoImmutablePreboundNestedHolder(inner, "root")
    db0.tags(obj).add("keep-prebound-embedded")

    assert obj.nested.name == "prebound child"
    assert inner.name == "prebound child"
    assert inner.count == 8
    assert isinstance(inner, MemoImmutableNestedPayload)
    assert db0.is_memo(inner)
    with pytest.raises(Exception):
        db0.uuid(inner)
    
