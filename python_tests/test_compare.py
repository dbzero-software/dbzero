# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from contextlib import ExitStack
from .memo_test_types import MemoTestClass, DynamicDataClass


def test_compare_two_versions_of_the_same_object(db0_fixture):
    obj_1 = MemoTestClass(9999)
    # assign tags to persist the object, otherwise it will not be accessible in snapshots
    db0.tags(obj_1).add("temp")
    db0.commit()
    snap_1 = db0.snapshot()
    obj_1.value = 100
    db0.commit()
    snap_2 = db0.snapshot()
    
    uuid = db0.uuid(obj_1)
    assert db0.compare(snap_1.fetch(uuid), snap_2.fetch(uuid)) == False


def test_compare_identical_versions(db0_fixture):
    obj_1 = MemoTestClass(9999)
    # assign tags to persist the object, otherwise it will not be accessible in snapshots
    db0.tags(obj_1).add("temp")    
    db0.commit()
    snap_1 = db0.snapshot()
    obj_1.value = 100
    db0.commit()
    obj_1.value = 9999
    db0.commit()
    snap_2 = db0.snapshot()
    
    uuid = db0.uuid(obj_1)
    assert db0.compare(snap_1.fetch(uuid), snap_2.fetch(uuid)) == True


def test_compare_index_vt_mutations(db0_fixture):
    _ = DynamicDataClass(120)
    obj_1 = DynamicDataClass([0, 1, 2, 11, 33, 119])
    # assign tags to persist the object, otherwise it will not be accessible in snapshots
    db0.tags(obj_1).add("temp")
    index_vt = db0.describe(obj_1)["field_layout"]["index_vt"]
    db0.commit()
    snap_1 = db0.snapshot()
    # mutate only index-vt fields
    for key in index_vt.keys():        
        setattr(obj_1, f"field_{key}", 0)
    db0.commit()
    snap_2 = db0.snapshot()
    
    uuid = db0.uuid(obj_1)
    assert db0.compare(snap_1.fetch(uuid), snap_2.fetch(uuid)) == False
    
    
def test_compare_kv_index_mutations(db0_fixture):
    obj_1 = MemoTestClass(9999)
    obj_1.new_field_1 = 100
    # assign tags to persist the object, otherwise it will not be accessible in snapshots
    db0.tags(obj_1).add("temp")
    db0.commit()
    snap_1 = db0.snapshot()    
    obj_1.new_field_2 = 200
    db0.commit()
    snap_2 = db0.snapshot()
    
    uuid = db0.uuid(obj_1)    
    assert db0.compare(snap_1.fetch(uuid), snap_2.fetch(uuid)) == False
    

def test_compare_instances_of_different_types(db0_fixture):
    obj_1 = MemoTestClass(9999)
    obj_2 = DynamicDataClass(10)
    assert db0.compare(obj_1, obj_2) == False
    

def test_compare_different_instances_of_same_type(db0_fixture):
    obj_1 = MemoTestClass(9999)
    obj_2 = MemoTestClass(9998)
    assert db0.compare(obj_1, obj_2) == False
    
    
def test_compare_identical_instances(db0_fixture):
    obj_1 = MemoTestClass(9999)
    obj_2 = MemoTestClass(9999)
    assert db0.compare(obj_1, obj_2) == True
    
    
def test_compare_instances_with_index_vt(db0_fixture):
    obj_ = DynamicDataClass(120)
    obj_1 = DynamicDataClass([0, 1, 2, 11, 33, 119])
    obj_2 = DynamicDataClass([0, 1, 2, 11, 33, 119])
    obj_3 = DynamicDataClass([0, 1, 3, 11, 33, 119])
    assert db0.compare(obj_1, obj_2) == True
    assert db0.compare(obj_1, obj_3) == False
    

def test_compare_instances_with_kv_index(db0_fixture):
    obj_1 = MemoTestClass(9999)
    obj_1.new_field_1 = 100
    obj_2 = MemoTestClass(9999)
    obj_2.new_field_1 = 100
    obj_3 = MemoTestClass(9999)
    obj_3.new_field_2 = 100    
    assert db0.compare(obj_1, obj_2) == True
    assert db0.compare(obj_1, obj_3) == False
    
    
def test_compare_same_object_but_different_tags(db0_fixture):
    obj_1 = MemoTestClass(9999)
    # assign tags to persist the object, otherwise it will not be accessible in snapshots
    db0.tags(obj_1).add("temp")
    db0.commit()
    snap_1 = db0.snapshot()
    db0.tags(obj_1).add("tag_1")
    db0.commit()
    snap_2 = db0.snapshot()
    db0.tags(obj_1).remove("tag_1")
    db0.commit()
    snap_3 = db0.snapshot()
    
    uuid = db0.uuid(obj_1)
    # compare without tags
    assert db0.compare(snap_1.fetch(uuid), snap_2.fetch(uuid)) == True    
    # compare with missing tags
    assert db0.compare(snap_1.fetch(uuid), snap_2.fetch(uuid), tags=["SOME-TAG"]) == True    
    # compare with updated tags
    assert db0.compare(snap_1.fetch(uuid), snap_2.fetch(uuid), tags=["tag_1"]) == False
    assert db0.compare(snap_1.fetch(uuid), snap_3.fetch(uuid), tags=["tag_1"]) == True
    