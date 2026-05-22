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
class MemoImmutableTupleHolder:
    def __init__(self, payload):
        self.payload = payload


@db0.memo(immutable=True, no_default_tags=True)
class MemoImmutableSetHolder:
    def __init__(self, payload):
        self.payload = payload


@db0.memo(immutable=True, no_default_tags=True)
class MemoImmutableDictHolder:
    def __init__(self, payload):
        self.payload = payload


@db0.memo(no_default_tags=True)
class MemoSetReferenceHolder:
    def __init__(self, payload):
        self.payload = payload


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
    assert isinstance(hash(inner), int)

    obj = MemoImmutablePreboundNestedHolder(inner, "root")
    db0.tags(obj).add("keep-prebound-embedded")

    assert obj.nested.name == "prebound child"
    assert inner.name == "prebound child"
    assert inner.count == 8
    assert isinstance(inner, MemoImmutableNestedPayload)
    assert db0.is_memo(inner)
    assert db0.uuid(inner) != db0.uuid(obj)


@pytest.mark.skip(
    reason="TODO: embedded immutable references can be created, but unreferencing by inner address is not implemented yet."
)
def test_regular_memo_can_reference_embedded_immutable_nested_object(db0_fixture):
    outer = MemoImmutableNestedHolder(name="referenced child", count=21, label="root")
    db0.tags(outer).add("keep-reference-source")
    inner = outer.nested

    holder = MemoSetReferenceHolder(inner)

    assert db0.uuid(inner) != db0.uuid(outer)
    assert db0.uuid(holder)


@pytest.mark.skip(
    reason="TODO: embedded immutable references can be created, but unreferencing by inner address is not implemented yet."
)
def test_db0_collections_can_store_embedded_immutable_nested_object_reference(db0_fixture):
    outer = MemoImmutableNestedHolder(name="collection child", count=22, label="root")
    db0.tags(outer).add("keep-collection-source")
    inner = outer.nested

    list_holder = MemoSetReferenceHolder(db0.list([inner]))
    set_holder = MemoSetReferenceHolder(db0.set([inner]))
    dict_holder = MemoSetReferenceHolder(db0.dict({"child": inner, inner: "value"}))

    assert db0.uuid(inner) != db0.uuid(outer)
    assert db0.uuid(list_holder)
    assert db0.uuid(set_holder)
    assert db0.uuid(dict_holder)


@pytest.mark.skip(
    reason="TODO: embedded immutable references can be created, but unreferencing by inner address is not implemented yet."
)
def test_index_can_store_embedded_immutable_nested_object_reference(db0_fixture):
    outer = MemoImmutableNestedHolder(name="index child", count=23, label="root")
    db0.tags(outer).add("keep-index-source")
    inner = outer.nested
    index = db0.index()

    index.add(1, inner)
    index.flush()

    assert db0.uuid(inner) != db0.uuid(outer)
    assert len(index) == 1


def test_read_embedded_tuple_field(db0_fixture):
    payload = tuple(f"alpha-{index}" for index in range(12)) + (7, b"bytes", None)
    obj = MemoImmutableTupleHolder(payload)
    db0.tags(obj).add("keep-embedded-tuple")

    assert type(obj.payload).__name__ == "EmbeddedTuple"
    assert len(obj.payload) == len(payload)
    assert obj.payload[0] == "alpha-0"
    assert obj.payload[12] == 7
    assert obj.payload[-2] == b"bytes"
    assert obj.payload.count("alpha-3") == 1
    assert obj.payload.index("alpha-3") == 3
    assert tuple(obj.payload) == payload
    assert repr(obj.payload) == repr(payload)


def test_embedded_list_field_is_exposed_as_embedded_tuple(db0_fixture):
    payload = [f"alpha-{index}" for index in range(12)] + [7]
    obj = MemoImmutableTupleHolder(payload)
    db0.tags(obj).add("keep-embedded-list")

    assert type(obj.payload).__name__ == "EmbeddedTuple"
    assert tuple(obj.payload) == tuple(payload)


def test_embedded_tuple_with_prebound_immutable_object_element(db0_fixture):
    inner = MemoImmutableNestedPayload(name="tuple child", count=11)
    assert isinstance(hash(inner), int)

    obj = MemoImmutableTupleHolder(("prefix", inner))
    db0.tags(obj).add("keep-embedded-tuple-object")

    assert obj.payload[0] == "prefix"
    assert obj.payload[1].name == "tuple child"
    assert obj.payload[1].count == 11
    assert inner.name == "tuple child"
    assert inner.count == 11
    assert isinstance(inner, MemoImmutableNestedPayload)
    assert db0.is_memo(inner)
    assert db0.uuid(inner) != db0.uuid(obj)


def test_read_embedded_set_field_after_reopen(db0_fixture):
    payload = {"alpha", "beta", 7, b"bytes", None}
    obj = MemoImmutableSetHolder(payload)
    db0.tags(obj).add("keep-embedded-set")
    obj_id = db0.uuid(obj)

    assert type(obj.payload).__name__ == "EmbeddedSet"
    assert len(obj.payload) == len(payload)
    assert "alpha" in obj.payload
    assert b"bytes" in obj.payload
    assert set(obj.payload) == payload

    del obj
    gc.collect()
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened = db0.fetch(obj_id)
    assert type(reopened.payload).__name__ == "EmbeddedSet"
    assert set(reopened.payload) == payload


def test_embedded_set_with_prebound_immutable_object_element(db0_fixture):
    inner = MemoImmutableNestedPayload(name="set child", count=13)
    obj = MemoImmutableSetHolder({inner, "marker"})
    db0.tags(obj).add("keep-embedded-set-object")

    values = list(obj.payload)
    embedded_inner = next(item for item in values if isinstance(item, MemoImmutableNestedPayload))
    assert "marker" in obj.payload
    assert embedded_inner.name == "set child"
    assert embedded_inner.count == 13
    assert inner.name == "set child"
    assert inner.count == 13
    assert isinstance(inner, MemoImmutableNestedPayload)
    assert db0.is_memo(inner)
    assert db0.uuid(inner) != db0.uuid(obj)


def test_python_set_lookup_survives_prebound_immutable_object_embedding(db0_fixture):
    inner = MemoImmutableNestedPayload(name="python set embedded child", count=31)
    values = {inner, "marker"}
    obj = MemoImmutableSetHolder(values)
    db0.tags(obj).add("keep-python-set-lookup-after-embedding")

    assert inner in values
    assert "marker" in values
    assert inner.name == "python set embedded child"
    assert db0.uuid(inner) != db0.uuid(obj)


def test_python_set_accepts_transient_immutable_object(db0_fixture):
    inner = MemoImmutableNestedPayload(name="python set child", count=17)
    values = {inner, "marker"}

    assert inner in values
    assert "marker" in values


def test_db0_set_rejects_transient_immutable_object(db0_fixture):
    inner = MemoImmutableNestedPayload(name="db0 set child", count=19)
    with pytest.raises(Exception):
        db0.set([inner])


def test_db0_set_uses_durable_hash_for_materialized_immutable_after_reopen(db0_fixture):
    obj = MemoImmutableClass1(data="durable set immutable", value=29)
    db0.tags(obj).add("keep-durable-set-immutable")
    obj_id = db0.uuid(obj)

    holder = MemoSetReferenceHolder(db0.set([obj]))
    db0.tags(holder).add("keep-durable-set-holder")
    holder_id = db0.uuid(holder)

    assert obj in holder.payload

    del obj
    del holder
    gc.collect()
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened_obj = db0.fetch(obj_id)
    reopened_holder = db0.fetch(holder_id)
    assert reopened_obj in reopened_holder.payload


def test_read_embedded_dict_field_after_reopen(db0_fixture):
    payload = {"name": "dbzero", "count": 3, 7: b"bytes", None: True}
    obj = MemoImmutableDictHolder(payload)
    db0.tags(obj).add("keep-embedded-dict")
    obj_id = db0.uuid(obj)

    assert type(obj.payload).__name__ == "EmbeddedDict"
    assert len(obj.payload) == len(payload)
    assert "name" in obj.payload
    assert obj.payload["name"] == "dbzero"
    assert obj.payload.get("count") == 3
    assert obj.payload.get("missing", "fallback") == "fallback"
    keys = obj.payload.keys()
    assert not isinstance(keys, tuple)
    assert iter(keys) is keys
    assert not isinstance(obj.payload.values(), tuple)
    assert not isinstance(obj.payload.items(), tuple)
    assert set(obj.payload.keys()) == set(payload.keys())
    assert set(obj.payload.values()) == set(payload.values())
    assert set(obj.payload.items()) == set(payload.items())
    assert dict(obj.payload) == payload
    assert repr(obj.payload) == repr(payload)

    del obj
    gc.collect()
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened = db0.fetch(obj_id)
    assert type(reopened.payload).__name__ == "EmbeddedDict"
    assert dict(reopened.payload) == payload


def test_embedded_dict_numeric_key_lookup_uses_python_equality(db0_fixture):
    bool_key = MemoImmutableDictHolder({True: "bool-key"})
    int_key = MemoImmutableDictHolder({1: "int-key"})

    assert bool_key.payload[1] == "bool-key"
    assert bool_key.payload[1.0] == "bool-key"
    assert bool_key.payload.get(1) == "bool-key"
    assert 1 in bool_key.payload
    assert int_key.payload[True] == "int-key"
    assert int_key.payload[1.0] == "int-key"
    assert int_key.payload.get(True) == "int-key"
    assert True in int_key.payload


def test_embedded_dict_lookup_rejects_unhashable_key(db0_fixture):
    obj = MemoImmutableDictHolder({"name": "dbzero"})

    with pytest.raises(TypeError):
        [] in obj.payload
    with pytest.raises(TypeError):
        obj.payload[[]]
    with pytest.raises(TypeError):
        obj.payload.get([])


def test_embedded_dict_with_prebound_immutable_object_value(db0_fixture):
    inner = MemoImmutableNestedPayload(name="dict child", count=37)
    obj = MemoImmutableDictHolder({"child": inner, "marker": "value"})
    db0.tags(obj).add("keep-embedded-dict-value-object")

    embedded_inner = obj.payload["child"]
    assert embedded_inner.name == "dict child"
    assert embedded_inner.count == 37
    assert inner.name == "dict child"
    assert inner.count == 37
    assert isinstance(inner, MemoImmutableNestedPayload)
    assert db0.is_memo(inner)
    assert db0.uuid(inner) != db0.uuid(obj)


def test_embedded_dict_with_prebound_immutable_object_key(db0_fixture):
    inner = MemoImmutableNestedPayload(name="dict key child", count=41)
    payload = {inner: "child-value", "marker": "value"}
    obj = MemoImmutableDictHolder(payload)
    db0.tags(obj).add("keep-embedded-dict-key-object")

    assert inner in payload
    assert obj.payload[inner] == "child-value"
    assert inner in obj.payload
    assert inner.name == "dict key child"
    assert inner.count == 41
    assert db0.uuid(inner) != db0.uuid(obj)


def test_embedded_dict_recursively_exposes_nested_collections(db0_fixture):
    inner = MemoImmutableNestedPayload(name="nested dict child", count=43)
    payload = {"nested": {"tuple": ("prefix", inner), "set": {"marker", 5}}}
    obj = MemoImmutableDictHolder(payload)
    db0.tags(obj).add("keep-embedded-dict-nested")

    nested = obj.payload["nested"]
    assert type(nested).__name__ == "EmbeddedDict"
    assert tuple(nested["tuple"])[0] == "prefix"
    embedded_inner = nested["tuple"][1]
    assert embedded_inner.name == "nested dict child"
    assert embedded_inner.count == 43
    assert set(nested["set"]) == {"marker", 5}
    
