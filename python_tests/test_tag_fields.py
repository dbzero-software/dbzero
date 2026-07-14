# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, KVTestClass


@pytest.mark.skip(reason="This test fails on manylinux")
def test_fields_can_be_retrieved_from_memo_type(db0_fixture):
    fields = MemoTestClass.__fields__
    assert fields is not None    

@pytest.mark.skip(reason="This test fails on manylinux")   
def test_fields_can_be_used_to_access_memo_type_defs(db0_fixture):
    # an instance must be created to initialize type fields
    object = MemoTestClass(123)
    field_def = MemoTestClass.__fields__.value
    assert field_def is not None

@pytest.mark.skip(reason="This test fails on manylinux")
def test_scoped_tags_can_be_assigned_with_field_def(db0_fixture):
    # an instance must be created to initialize type fields
    object = KVTestClass(1)
    # add tags using field scope
    db0.tags(object).add("tag1", (KVTestClass.__fields__.key, "tag1"))
    db0.tags(object).add((KVTestClass.__fields__.value, "tag2"))
    
@pytest.mark.skip(reason="This test fails on manylinux") 
def test_long_tags_can_be_used_in_search(db0_fixture):
    db0.tags(KVTestClass(1)).add("tag1", (KVTestClass.__fields__.key, "tag1"))
    db0.tags(KVTestClass(2)).add((KVTestClass.__fields__.value, "tag1"))
    assert [x.key for x in db0.find(KVTestClass, (KVTestClass.__fields__.key, "tag1"))] == [1]
    assert [x.key for x in db0.find(KVTestClass, (KVTestClass.__fields__.value, "tag1"))] == [2]
