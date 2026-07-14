# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import DynamicDataClass, MemoTestClass

        
def test_pos_vt_created_for_consecutive_slots(db0_fixture):
    object_1 = DynamicDataClass(5)
    # NOTE: there might be more slots preallocated for lo-fi members
    assert len(db0.describe(object_1)["field_layout"]["pos_vt"]) >= 5
    object_2 = DynamicDataClass(10)
    assert len(db0.describe(object_2)["field_layout"]["pos_vt"]) >= 10
    
    
def test_index_vt_created_with_sparsely_filled_slots(db0_fixture):
    # 120 fields will be registered in the class field layout
    object_1 = DynamicDataClass(120)
    object_2 = DynamicDataClass([0, 1, 2, 11, 33, 119])
    # 3 fields should be placed in pos_vt
    # NOTE: there might be more slots preallocated for lo-fi members
    assert len(db0.describe(object_2)["field_layout"]["pos_vt"]) >= 3
    # the remaining 3 fields into index_vt
    index_vt = db0.describe(object_2)["field_layout"]["index_vt"]
    assert len(index_vt.keys()) == 3


def test_space_is_saved_with_sparse_storage(db0_fixture):
    object_1 = DynamicDataClass(120)
    size_per_field_1 = db0.describe(object_1)["size_of"] / 120
    object_2 = DynamicDataClass([0, 1, 2, 11, 33, 119])
    size_per_field_2 = db0.describe(object_2)["size_of"] / 120
    # saving should be at least 10x
    assert size_per_field_2 < size_per_field_1 * 10


def test_fields_can_be_pulled_from_index_vt(db0_fixture):
    object_1 = DynamicDataClass(120)
    object_2 = DynamicDataClass([0, 1, 2, 11, 33, 119])    
    assert object_2.field_33 == 33
    # exception should be raised for a non-exising field
    with pytest.raises(Exception):
        object_2.field_34


def test_fields_can_be_updated_in_index_vt(db0_fixture):
    object_1 = DynamicDataClass(120)
    object_2 = DynamicDataClass([0, 1, 2, 11, 33, 119])
    object_2.field_33 = 100
    assert object_2.field_33 == 100
    
    
def test_fields_can_be_renamed_programmatically(db0_fixture):
    object_1 = DynamicDataClass(120)
    assert object_1.field_33 == 33
    # rename field in this class
    db0.rename_field(DynamicDataClass, "field_33", "new_fancy_name")
    # make sure field can be accessed by a new name
    assert object_1.new_fancy_name == 33


def test_double_field_rename_should_not_fail_by_default(db0_fixture):
    object_1 = DynamicDataClass(120)    
    db0.rename_field(DynamicDataClass, "field_33", "new_fancy_name")
    db0.rename_field(DynamicDataClass, "field_33", "new_fancy_name")


def test_fields_can_be_added_to_existing_object(db0_fixture):
    object_1 = DynamicDataClass(5)
    object_1.field_60 = 60


def test_can_read_dynamic_field(db0_fixture):
    object_1 = DynamicDataClass(5)
    object_1.field_60 = 60
    assert object_1.field_60 == 60
    

def test_dynamic_fields_can_be_mutated(db0_fixture):
    object_1 = DynamicDataClass(5)
    object_1.field_60 = 60
    assert object_1.field_60 == 60
    object_1.field_60 = 120
    assert object_1.field_60 == 120
