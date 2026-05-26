# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from random import random
import pytest
import dbzero as db0
from dataclasses import dataclass
from .conftest import DB0_DIR
import random
import gc


OBJECT_REF_STORAGE_CLASS = 13
UNIQUE_ADDRESS_INSTANCE_ID_SHIFT = 14
BASE32_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"


def _base32_encode(data):
    table = (
        (0b11111000, 3), (0b00000111, -2), (0b11000000, 6), (0b00111110, 1),
        (0b00000001, -4), (0b11110000, 4), (0b00001111, -1), (0b10000000, 7),
        (0b01111100, 2), (0b00000011, -3), (0b11100000, 5), (0b00011111, 0),
    )
    strides = (1, 2, 1, 2, 2, 1, 2, 1)
    result = []
    data_at = 0
    table_at = 0
    stride_at = 0
    while data_at < len(data):
        enc_value = 0
        in_val = data[data_at]
        for _ in range(strides[stride_at]):
            mask, shift = table[table_at]
            if shift > 0:
                enc_value |= (in_val & mask) >> shift
            else:
                enc_value |= (in_val & mask) << -shift
                data_at += 1
                in_val = data[data_at] if data_at != len(data) else 0
            table_at = (table_at + 1) % len(table)
        result.append(BASE32_CHARS[enc_value])
        stride_at = (stride_at + 1) % len(strides)
    return "".join(result)


def _base32_decode(data):
    table = (
        (0b00011111, 3), (0b00011100, -2), (0b00000011, 6), (0b00011111, 1),
        (0b00010000, -4), (0b00001111, 4), (0b00011110, -1), (0b00000001, 7),
        (0b00011111, 2), (0b00011000, -3), (0b00000111, 5), (0b00011111, 0),
    )
    strides = (2, 3, 2, 3, 2)
    result = bytearray([0])
    table_at = 0
    stride_at = 0
    stride = strides[stride_at]
    for char_at, char in enumerate(data):
        value = BASE32_CHARS.index(char)
        while True:
            mask, shift = table[table_at]
            if shift >= 0:
                result[-1] |= (value & mask) << shift
                table_at = (table_at + 1) % len(table)
                stride -= 1
                if stride == 0 and char_at != len(data) - 1:
                    result.append(0)
                    stride_at = (stride_at + 1) % len(strides)
                    stride = strides[stride_at]
                break
            result[-1] |= (value & mask) >> -shift
            table_at = (table_at + 1) % len(table)
            stride -= 1
            if stride == 0:
                result.append(0)
                stride_at += 1
                stride = strides[stride_at]
    return bytes(result)


def _write_packed_int(value):
    result = bytearray([value & 0x7F])
    value >>= 7
    while value:
        result.insert(0, (value & 0x7F) | 0x80)
        value >>= 7
    return bytes(result)


def _read_packed_int(data, at):
    value = 0
    while data[at] & 0x80:
        value |= data[at] & 0x7F
        value <<= 7
        at += 1
    value |= data[at] & 0x7F
    return value, at + 1


def _decode_uuid(uuid):
    data = _base32_decode(uuid)
    fixture_uuid = int.from_bytes(data[:8], "little")
    unique_address, at = _read_packed_int(data, 8)
    storage_class, _ = _read_packed_int(data, at)
    return fixture_uuid, unique_address, storage_class


def _encode_uuid(fixture_uuid, unique_address, storage_class):
    data = (
        fixture_uuid.to_bytes(8, "little")
        + _write_packed_int(unique_address)
        + _write_packed_int(storage_class)
    )
    return _base32_encode(data)


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


@db0.memo(no_default_tags=True)
class MemoRegularFetchUUIDPayload:
    def __init__(self, name, count):
        self.name = name
        self.count = count


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
class MemoImmutableDeepLeaf:
    def __init__(self, name, count):
        self.name = name
        self.count = count


@db0.memo(immutable=True, no_default_tags=True)
class MemoImmutableDeepMiddle:
    def __init__(self, name, count):
        self.name = name
        self.leaf = MemoImmutableDeepLeaf(name=f"{name}-leaf", count=count)


@db0.memo(immutable=True, no_default_tags=True)
class MemoImmutableDeepRoot:
    def __init__(self, name, count):
        self.middle = MemoImmutableDeepMiddle(name=name, count=count)
        self.label = "deep-root"


@db0.memo(immutable=True)
class MemoImmutableDefaultTagLeaf:
    def __init__(self, name, count):
        self.name = name
        self.count = count


@db0.memo(immutable=True)
class MemoImmutableDefaultTagMiddle:
    def __init__(self, name, count):
        self.name = name
        self.leaf = MemoImmutableDefaultTagLeaf(name=f"{name}-leaf", count=count)


@db0.memo(immutable=True)
class MemoImmutableDefaultTagRoot:
    def __init__(self, name, count):
        self.middle = MemoImmutableDefaultTagMiddle(name=name, count=count)
        self.label = "default-tag-root"


@db0.memo(immutable=True)
class MemoImmutableDefaultTagBase:
    def __init__(self, name):
        self.name = name


@db0.memo(immutable=True)
class MemoImmutableDefaultTagDerived(MemoImmutableDefaultTagBase):
    def __init__(self, name, count):
        super().__init__(name)
        self.count = count


@db0.memo(immutable=True)
class MemoImmutableDefaultTagDerivedLeaf(MemoImmutableDefaultTagDerived):
    def __init__(self, name, count, marker):
        super().__init__(name=name, count=count)
        self.marker = marker


@db0.memo(immutable=True)
class MemoImmutableDefaultTagInheritanceRoot:
    def __init__(self, name, count):
        self.child = MemoImmutableDefaultTagDerived(name=name, count=count)
        self.leaf = MemoImmutableDefaultTagDerivedLeaf(
            name=f"{name}-leaf", count=count + 1, marker="deep-derived"
        )


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


def test_uuid_and_fetch_regular_memo_object(db0_fixture):
    obj = MemoRegularFetchUUIDPayload("regular uuid", 101)
    obj_uuid = db0.uuid(obj)

    assert db0.fetch(obj_uuid) is obj
    assert db0.fetch(MemoRegularFetchUUIDPayload, obj_uuid) is obj

    db0.tags(obj).add("keep-regular-fetch-uuid")
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened = db0.fetch(obj_uuid)
    assert isinstance(reopened, MemoRegularFetchUUIDPayload)
    assert reopened.name == "regular uuid"
    assert reopened.count == 101


def test_uuid_and_fetch_immutable_root_object(db0_fixture):
    obj = MemoImmutableClass1(data="immutable uuid", value=102)
    db0.tags(obj).add("keep-immutable-fetch-uuid")
    obj_uuid = db0.uuid(obj)

    assert db0.fetch(obj_uuid) is obj
    assert db0.fetch(MemoImmutableClass1, obj_uuid) is obj

    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened = db0.fetch(obj_uuid)
    assert isinstance(reopened, MemoImmutableClass1)
    assert reopened.data == "immutable uuid"
    assert reopened.value == 102


def test_uuid_and_fetch_embedded_nested_immutable_object(db0_fixture):
    root = MemoImmutableNestedHolder(name="embedded uuid", count=103, label="root")
    db0.tags(root).add("keep-embedded-fetch-uuid")
    nested = root.nested
    nested_uuid = db0.uuid(nested)

    assert nested_uuid != db0.uuid(root)
    fetched = db0.fetch(nested_uuid)
    fetched_by_type = db0.fetch(MemoImmutableNestedPayload, nested_uuid)
    assert isinstance(fetched, MemoImmutableNestedPayload)
    assert fetched.name == "embedded uuid"
    assert fetched.count == 103
    assert db0.uuid(fetched) == nested_uuid
    assert db0.uuid(fetched_by_type) == nested_uuid

    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened = db0.fetch(nested_uuid)
    assert isinstance(reopened, MemoImmutableNestedPayload)
    assert reopened.name == "embedded uuid"
    assert reopened.count == 103


def test_embedded_immutable_shadow_type_reused_for_public_operations(db0_fixture):
    root_a = db0.materialized(MemoImmutableNestedHolder(name="embedded type cache a", count=131, label="root-a"))
    root_b = db0.materialized(MemoImmutableNestedHolder(name="embedded type cache b", count=132, label="root-b"))
    nested_a = root_a.nested
    nested_b = root_b.nested

    assert isinstance(nested_a, MemoImmutableNestedPayload)
    assert type(nested_a) is type(nested_b)

    nested_uuid = db0.uuid(nested_a)
    db0.tags(nested_a).add("embedded-type-cache-tag")
    result = list(db0.find(MemoImmutableNestedPayload, "embedded-type-cache-tag"))

    assert len(result) == 1
    assert db0.uuid(result[0]) == nested_uuid
    assert result[0].name == "embedded type cache a"


def test_uuid_and_fetch_deeply_embedded_immutable_objects(db0_fixture):
    root = MemoImmutableDeepRoot(name="deep embedded uuid", count=104)
    db0.tags(root).add("keep-deep-embedded-fetch-uuid")
    middle = root.middle
    leaf = middle.leaf
    middle_uuid = db0.uuid(middle)
    leaf_uuid = db0.uuid(leaf)

    assert middle_uuid != db0.uuid(root)
    assert leaf_uuid != middle_uuid
    fetched_middle = db0.fetch(middle_uuid)
    fetched_leaf = db0.fetch(leaf_uuid)
    assert isinstance(fetched_middle, MemoImmutableDeepMiddle)
    assert isinstance(fetched_leaf, MemoImmutableDeepLeaf)
    assert fetched_middle.name == "deep embedded uuid"
    assert fetched_leaf.name == "deep embedded uuid-leaf"
    assert db0.uuid(fetched_middle) == middle_uuid
    assert db0.uuid(fetched_leaf) == leaf_uuid

    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened_middle = db0.fetch(middle_uuid)
    reopened_leaf = db0.fetch(leaf_uuid)
    assert isinstance(reopened_middle, MemoImmutableDeepMiddle)
    assert isinstance(reopened_leaf, MemoImmutableDeepLeaf)
    assert reopened_middle.name == "deep embedded uuid"
    assert reopened_middle.leaf.name == "deep embedded uuid-leaf"
    assert reopened_leaf.name == "deep embedded uuid-leaf"
    assert reopened_leaf.count == 104


def test_fetch_rejects_invalid_embedded_uuid_inside_existing_allocation(db0_fixture):
    root = MemoImmutableDeepRoot(name="bad embedded uuid", count=105)
    db0.tags(root).add("keep-invalid-embedded-fetch-uuid")
    leaf_uuid = db0.uuid(root.middle.leaf)
    fixture_uuid, unique_address, storage_class = _decode_uuid(leaf_uuid)
    assert storage_class == OBJECT_REF_STORAGE_CLASS

    address = unique_address >> UNIQUE_ADDRESS_INSTANCE_ID_SHIFT
    instance_id = unique_address & ((1 << UNIQUE_ADDRESS_INSTANCE_ID_SHIFT) - 1)
    invalid_unique_address = ((address + 1) << UNIQUE_ADDRESS_INSTANCE_ID_SHIFT) | instance_id
    invalid_uuid = _encode_uuid(fixture_uuid, invalid_unique_address, storage_class)

    with pytest.raises(Exception):
        db0.fetch(invalid_uuid)


def test_tag_and_find_immutable_instance(db0_fixture):
    obj_1 = MemoImmutableClass1(data="immutable data", value=42)
    db0.tags(obj_1).add("tag1", "tag2")
    assert list(db0.find("tag1")) == [obj_1]


def test_tag_and_find_embedded_immutable_instance(db0_fixture):
    root = MemoImmutableNestedHolder(name="tagged embedded child", count=106, label="root")
    root = db0.materialized(root)
    root_uuid = db0.uuid(root)
    nested = root.nested
    nested_uuid = db0.uuid(nested)

    db0.tags(nested).add("embedded-tag")

    result = list(db0.find("embedded-tag"))
    assert len(result) == 1
    assert isinstance(result[0], MemoImmutableNestedPayload)
    assert result[0].name == "tagged embedded child"
    assert result[0].count == 106
    assert db0.uuid(result[0]) == nested_uuid
    assert db0.uuid(result[0]) != root_uuid


def test_typed_find_embedded_immutable_instance(db0_fixture):
    root = MemoImmutableNestedHolder(name="typed tagged embedded child", count=107, label="root")
    root = db0.materialized(root)
    db0.uuid(root)
    nested_uuid = db0.uuid(root.nested)

    db0.tags(root.nested).add("embedded-typed-tag")

    typed_result = list(db0.find(MemoImmutableNestedPayload, "embedded-typed-tag"))
    root_result = list(db0.find(MemoImmutableNestedHolder, "embedded-typed-tag"))
    assert len(typed_result) == 1
    assert typed_result[0].name == "typed tagged embedded child"
    assert db0.uuid(typed_result[0]) == nested_uuid
    assert root_result == []


def test_tagged_embedded_immutable_instance_survives_reopen(db0_fixture):
    root = MemoImmutableNestedHolder(name="reopened tagged embedded child", count=108, label="root")
    root = db0.materialized(root)
    db0.uuid(root)
    nested_uuid = db0.uuid(root.nested)
    db0.tags(root.nested).add("embedded-reopen-tag")

    del root
    gc.collect()
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    result = list(db0.find("embedded-reopen-tag"))
    assert len(result) == 1
    assert isinstance(result[0], MemoImmutableNestedPayload)
    assert result[0].name == "reopened tagged embedded child"
    assert result[0].count == 108
    assert db0.uuid(result[0]) == nested_uuid


def test_tag_and_find_deep_embedded_immutable_instance(db0_fixture):
    root = MemoImmutableDeepRoot(name="deep tagged embedded", count=109)
    root = db0.materialized(root)
    db0.uuid(root)
    leaf_uuid = db0.uuid(root.middle.leaf)

    db0.tags(root.middle.leaf).add("embedded-deep-tag")

    result = list(db0.find(MemoImmutableDeepLeaf, "embedded-deep-tag"))
    assert len(result) == 1
    assert result[0].name == "deep tagged embedded-leaf"
    assert result[0].count == 109
    assert db0.uuid(result[0]) == leaf_uuid


def test_find_embedded_immutable_instances_by_default_type_tags(db0_fixture):
    root = db0.materialized(MemoImmutableDefaultTagRoot(name="type-only embedded", count=125))
    root_uuid = db0.uuid(root)
    middle_uuid = db0.uuid(root.middle)
    leaf_uuid = db0.uuid(root.middle.leaf)

    root_result = list(db0.find(MemoImmutableDefaultTagRoot))
    middle_result = list(db0.find(MemoImmutableDefaultTagMiddle))
    leaf_result = list(db0.find(MemoImmutableDefaultTagLeaf))

    assert [db0.uuid(item) for item in root_result] == [root_uuid]
    assert [db0.uuid(item) for item in middle_result] == [middle_uuid]
    assert middle_result[0].name == "type-only embedded"
    assert [db0.uuid(item) for item in leaf_result] == [leaf_uuid]
    assert leaf_result[0].name == "type-only embedded-leaf"
    assert leaf_result[0].count == 125


def test_find_embedded_immutable_instances_by_type_respects_no_default_tags(db0_fixture):
    root = db0.materialized(MemoImmutableDeepRoot(name="no default type-only embedded", count=126))
    assert root.middle.leaf.count == 126

    assert list(db0.find(MemoImmutableDeepRoot)) == []
    assert list(db0.find(MemoImmutableDeepMiddle)) == []
    assert list(db0.find(MemoImmutableDeepLeaf)) == []


def test_find_embedded_immutable_instances_by_base_type_default_tags(db0_fixture):
    root = db0.materialized(MemoImmutableDefaultTagInheritanceRoot(name="base type embedded", count=127))
    child_uuid = db0.uuid(root.child)
    leaf_uuid = db0.uuid(root.leaf)

    base_result = list(db0.find(MemoImmutableDefaultTagBase))
    derived_result = list(db0.find(MemoImmutableDefaultTagDerived))
    leaf_result = list(db0.find(MemoImmutableDefaultTagDerivedLeaf))

    assert [db0.uuid(item) for item in base_result] == [child_uuid, leaf_uuid]
    assert [db0.uuid(item) for item in derived_result] == [child_uuid, leaf_uuid]
    assert [db0.uuid(item) for item in leaf_result] == [leaf_uuid]
    assert isinstance(base_result[0], MemoImmutableDefaultTagDerived)
    assert base_result[0].name == "base type embedded"
    assert base_result[0].count == 127
    assert isinstance(base_result[1], MemoImmutableDefaultTagDerivedLeaf)
    assert base_result[1].name == "base type embedded-leaf"
    assert base_result[1].count == 128
    assert base_result[1].marker == "deep-derived"


def test_find_mixed_regular_immutable_and_embedded_tagged_instances(db0_fixture):
    tag = "mixed-tagged-memo-instances"
    regular = MemoRegularFetchUUIDPayload(name="mixed regular", count=111)
    immutable = db0.materialized(MemoImmutableClass1(data="mixed immutable", value=112))
    shallow_root = db0.materialized(MemoImmutableNestedHolder(
        name="mixed shallow embedded", count=113, label="root"
    ))
    deep_root = db0.materialized(MemoImmutableDeepRoot(name="mixed deep embedded", count=114))
    shallow_embedded = shallow_root.nested
    deep_embedded = deep_root.middle.leaf

    regular_uuid = db0.uuid(regular)
    immutable_uuid = db0.uuid(immutable)
    shallow_uuid = db0.uuid(shallow_embedded)
    deep_uuid = db0.uuid(deep_embedded)
    expected_uuids = {regular_uuid, immutable_uuid, shallow_uuid, deep_uuid}

    db0.tags(regular).add(tag)
    db0.tags(immutable).add(tag)
    db0.tags(shallow_embedded).add(tag)
    db0.tags(deep_embedded).add(tag)

    result = list(db0.find(tag))
    by_uuid = {db0.uuid(item): item for item in result}
    assert set(by_uuid) == expected_uuids

    assert isinstance(by_uuid[db0.uuid(regular)], MemoRegularFetchUUIDPayload)
    assert by_uuid[db0.uuid(regular)].name == "mixed regular"
    assert isinstance(by_uuid[db0.uuid(immutable)], MemoImmutableClass1)
    assert by_uuid[db0.uuid(immutable)].data == "mixed immutable"
    assert isinstance(by_uuid[db0.uuid(shallow_embedded)], MemoImmutableNestedPayload)
    assert by_uuid[db0.uuid(shallow_embedded)].name == "mixed shallow embedded"
    assert isinstance(by_uuid[db0.uuid(deep_embedded)], MemoImmutableDeepLeaf)
    assert by_uuid[db0.uuid(deep_embedded)].name == "mixed deep embedded-leaf"

    assert [db0.uuid(item) for item in db0.find(shallow_embedded, tag)] == [db0.uuid(shallow_embedded)]
    assert [db0.uuid(item) for item in db0.find(deep_embedded, tag)] == [db0.uuid(deep_embedded)]


def test_remove_tag_from_embedded_immutable_instance(db0_fixture):
    root = MemoImmutableNestedHolder(name="remove embedded child", count=110, label="root")
    root = db0.materialized(root)
    nested_uuid = db0.uuid(root.nested)
    db0.tags(root.nested).add("embedded-remove-unsupported-tag")

    assert [db0.uuid(item) for item in db0.find("embedded-remove-unsupported-tag")] == [nested_uuid]
    assert [db0.uuid(item) for item in db0.find(
        MemoImmutableNestedPayload, "embedded-remove-unsupported-tag"
    )] == [nested_uuid]

    db0.tags(root.nested).remove("embedded-remove-unsupported-tag")

    assert list(db0.find("embedded-remove-unsupported-tag")) == []
    assert list(db0.find(MemoImmutableNestedPayload, "embedded-remove-unsupported-tag")) == []


def test_remove_tags_from_deep_embedded_immutable_instance_with_iterable_and_operator(db0_fixture):
    root = db0.materialized(MemoImmutableDeepRoot(name="deep untag embedded", count=119))
    leaf_uuid = db0.uuid(root.middle.leaf)
    tags = db0.tags(root.middle.leaf)
    tags.add(["embedded-remove-one", "embedded-remove-two", "embedded-remove-three"])

    assert [db0.uuid(item) for item in db0.find("embedded-remove-one")] == [leaf_uuid]
    assert [db0.uuid(item) for item in db0.find("embedded-remove-two")] == [leaf_uuid]
    assert [db0.uuid(item) for item in db0.find("embedded-remove-three")] == [leaf_uuid]

    tags.remove(["embedded-remove-one", "embedded-remove-two"])
    tags -= "embedded-remove-three"

    assert list(db0.find("embedded-remove-one")) == []
    assert list(db0.find("embedded-remove-two")) == []
    assert list(db0.find("embedded-remove-three")) == []


def test_embedded_immutable_root_drops_after_last_tag_removed(db0_fixture):
    root = db0.materialized(MemoImmutableNestedHolder(
        name="drop after embedded untag", count=120, label="root"
    ))
    root_uuid = db0.uuid(root)
    nested = root.nested
    db0.tags(nested).add("embedded-only-keepalive")
    del nested
    del root
    gc.collect()

    db0.commit()
    assert db0.exists(root_uuid)

    nested = next(iter(db0.find("embedded-only-keepalive")))
    db0.tags(nested).remove("embedded-only-keepalive")
    del nested
    gc.collect()
    db0.commit()

    with pytest.raises(Exception):
        db0.fetch(root_uuid)


def test_removing_embedded_tags_preserves_other_mixed_tagged_instances(db0_fixture):
    tag = "mixed-tag-removal"
    regular = MemoRegularFetchUUIDPayload(name="mixed remove regular", count=121)
    immutable = db0.materialized(MemoImmutableClass1(data="mixed remove immutable", value=122))
    shallow_root = db0.materialized(MemoImmutableNestedHolder(
        name="mixed remove shallow embedded", count=123, label="root"
    ))
    deep_root = db0.materialized(MemoImmutableDeepRoot(name="mixed remove deep embedded", count=124))
    shallow_embedded = shallow_root.nested
    deep_embedded = deep_root.middle.leaf
    regular_uuid = db0.uuid(regular)
    immutable_uuid = db0.uuid(immutable)
    shallow_uuid = db0.uuid(shallow_embedded)
    deep_uuid = db0.uuid(deep_embedded)

    db0.tags(regular).add(tag)
    db0.tags(immutable).add(tag)
    db0.tags(shallow_embedded).add(tag)
    db0.tags(deep_embedded).add(tag)
    assert {db0.uuid(item) for item in db0.find(tag)} == {
        regular_uuid, immutable_uuid, shallow_uuid, deep_uuid
    }

    db0.tags(shallow_embedded).remove(tag)
    db0.tags(deep_embedded).remove(tag)

    assert {db0.uuid(item) for item in db0.find(tag)} == {regular_uuid, immutable_uuid}


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


def test_regular_memo_can_reference_embedded_immutable_nested_object(db0_fixture):
    outer = MemoImmutableNestedHolder(name="referenced child", count=21, label="root")
    db0.tags(outer).add("keep-reference-source")
    inner = outer.nested

    holder = MemoSetReferenceHolder(inner)
    db0.tags(holder).add("keep-regular-embedded-reference")
    holder_id = db0.uuid(holder)

    assert db0.uuid(inner) != db0.uuid(outer)
    assert db0.uuid(holder)

    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened = db0.fetch(holder_id)
    assert reopened.payload.name == "referenced child"
    assert reopened.payload.count == 21


def test_immutable_instance_drops_when_holding_memo_object_is_deleted(db0_fixture):
    obj = MemoImmutableClass1(data="held immutable root", value=31)
    db0.tags(obj).add("temporary-held-immutable-root")
    obj_id = db0.uuid(obj)
    holder = MemoSetReferenceHolder(obj)
    db0.tags(obj).remove("temporary-held-immutable-root")
    del obj
    gc.collect()

    db0.commit()
    assert db0.exists(obj_id)

    db0.delete(holder)
    del holder
    gc.collect()
    db0.commit()

    with pytest.raises(Exception):
        db0.fetch(obj_id)


def test_immutable_instance_supported_by_embedded_ref_drops_when_holding_memo_object_is_deleted(db0_fixture):
    outer = MemoImmutableNestedHolder(name="held embedded child", count=32, label="root")
    db0.tags(outer).add("temporary-held-embedded-root")
    outer_id = db0.uuid(outer)
    inner = outer.nested
    holder = MemoSetReferenceHolder(inner)
    db0.tags(outer).remove("temporary-held-embedded-root")
    del inner
    del outer
    gc.collect()

    db0.commit()
    assert db0.exists(outer_id)

    db0.delete(holder)
    del holder
    gc.collect()
    db0.commit()

    with pytest.raises(Exception):
        db0.fetch(outer_id)


def test_db0_collections_can_store_embedded_immutable_nested_object_reference(db0_fixture):
    outer = MemoImmutableNestedHolder(name="collection child", count=22, label="root")
    db0.tags(outer).add("keep-collection-source")
    inner = outer.nested

    list_holder = MemoSetReferenceHolder(db0.list([inner]))
    set_holder = MemoSetReferenceHolder(db0.set([inner]))
    dict_holder = MemoSetReferenceHolder(db0.dict({"child": inner, inner: "value"}))
    db0.tags(list_holder).add("keep-list-embedded-reference")
    db0.tags(set_holder).add("keep-set-embedded-reference")
    db0.tags(dict_holder).add("keep-dict-embedded-reference")
    list_holder_id = db0.uuid(list_holder)
    set_holder_id = db0.uuid(set_holder)
    dict_holder_id = db0.uuid(dict_holder)

    assert db0.uuid(inner) != db0.uuid(outer)
    assert db0.uuid(list_holder)
    assert db0.uuid(set_holder)
    assert db0.uuid(dict_holder)

    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    assert db0.fetch(list_holder_id).payload[0].name == "collection child"
    assert next(iter(db0.fetch(set_holder_id).payload)).count == 22
    reopened_dict = db0.fetch(dict_holder_id).payload
    assert reopened_dict["child"].name == "collection child"
    assert reopened_dict[reopened_dict["child"]] == "value"


def test_index_can_store_embedded_immutable_nested_object_reference(db0_fixture):
    outer = MemoImmutableNestedHolder(name="index child", count=23, label="root")
    db0.tags(outer).add("keep-index-source")
    inner = outer.nested
    index = db0.index()

    index.add(1, inner)
    index.flush()
    holder = MemoSetReferenceHolder(index)
    db0.tags(holder).add("keep-index-embedded-reference")
    holder_id = db0.uuid(holder)

    assert db0.uuid(inner) != db0.uuid(outer)
    assert len(index) == 1

    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened_index = db0.fetch(holder_id).payload
    retrieved = list(reopened_index.select())
    assert len(retrieved) == 1
    assert retrieved[0].name == "index child"
    assert retrieved[0].count == 23


def test_index_retrieves_mixed_regular_immutable_and_embedded_instances(db0_fixture):
    regular = MemoRegularFetchUUIDPayload(name="index mixed regular", count=115)
    immutable = db0.materialized(MemoImmutableClass1(data="index mixed immutable", value=116))
    shallow_root = db0.materialized(MemoImmutableNestedHolder(
        name="index mixed shallow embedded", count=117, label="root"
    ))
    deep_root = db0.materialized(MemoImmutableDeepRoot(name="index mixed deep embedded", count=118))
    shallow_embedded = shallow_root.nested
    deep_embedded = deep_root.middle.leaf

    regular_uuid = db0.uuid(regular)
    immutable_uuid = db0.uuid(immutable)
    shallow_uuid = db0.uuid(shallow_embedded)
    deep_uuid = db0.uuid(deep_embedded)
    expected_uuids = {regular_uuid, immutable_uuid, shallow_uuid, deep_uuid}

    index = db0.index()
    index.add(1, regular)
    index.add(2, immutable)
    index.add(3, shallow_embedded)
    index.add(4, deep_embedded)
    index.flush()

    retrieved = list(index.select())
    by_uuid = {db0.uuid(item): item for item in retrieved}
    assert set(by_uuid) == expected_uuids
    assert isinstance(by_uuid[regular_uuid], MemoRegularFetchUUIDPayload)
    assert by_uuid[regular_uuid].name == "index mixed regular"
    assert isinstance(by_uuid[immutable_uuid], MemoImmutableClass1)
    assert by_uuid[immutable_uuid].data == "index mixed immutable"
    assert isinstance(by_uuid[shallow_uuid], MemoImmutableNestedPayload)
    assert by_uuid[shallow_uuid].name == "index mixed shallow embedded"
    assert isinstance(by_uuid[deep_uuid], MemoImmutableDeepLeaf)
    assert by_uuid[deep_uuid].name == "index mixed deep embedded-leaf"

    holder = MemoSetReferenceHolder(index)
    db0.tags(holder).add("keep-index-mixed-retrieval")
    holder_id = db0.uuid(holder)
    del regular, immutable, shallow_embedded, deep_embedded, shallow_root, deep_root, index, holder
    gc.collect()

    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", "rw")

    reopened_index = db0.fetch(holder_id).payload
    reopened = list(reopened_index.select())
    reopened_by_uuid = {db0.uuid(item): item for item in reopened}
    assert set(reopened_by_uuid) == expected_uuids
    assert reopened_by_uuid[regular_uuid].name == "index mixed regular"
    assert reopened_by_uuid[immutable_uuid].data == "index mixed immutable"
    assert reopened_by_uuid[shallow_uuid].name == "index mixed shallow embedded"
    assert reopened_by_uuid[deep_uuid].name == "index mixed deep embedded-leaf"


def test_index_remove_unrefs_embedded_immutable_nested_object_reference(db0_fixture):
    outer = MemoImmutableNestedHolder(name="index remove child", count=24, label="root")
    db0.tags(outer).add("temporary-index-remove-source")
    inner = outer.nested
    outer_id = db0.uuid(outer)
    index = db0.index()

    index.add(1, inner)
    index.flush()
    db0.tags(outer).remove("temporary-index-remove-source")
    del outer
    del inner
    gc.collect()

    stored = list(index.select())[0]
    index.remove(1, stored)
    del stored
    gc.collect()
    db0.commit()

    with pytest.raises(Exception):
        db0.fetch(outer_id)


def test_index_clear_unrefs_embedded_immutable_nested_object_reference(db0_fixture):
    outer = MemoImmutableNestedHolder(name="index clear child", count=25, label="root")
    db0.tags(outer).add("temporary-index-clear-source")
    inner = outer.nested
    outer_id = db0.uuid(outer)
    index = db0.index()

    index.add(1, inner)
    db0.commit()
    db0.tags(outer).remove("temporary-index-clear-source")
    del outer
    del inner
    gc.collect()

    index.clear()
    db0.commit()

    with pytest.raises(Exception):
        db0.fetch(outer_id)


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
    
