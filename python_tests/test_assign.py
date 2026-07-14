# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestThreeParamsClass


def test_assign_multiple_attributes(db0_fixture):
    obj = MemoTestThreeParamsClass(1, 2, 3)
    db0.assign(obj, value_1=4, value_2=5, value_3=6)
    assert obj.value_1 == 4
    assert obj.value_2 == 5
    assert obj.value_3 == 6
    
    
def test_assign_multiple_attributes_on_multiple_instances(db0_fixture):
    obj_1 = MemoTestThreeParamsClass(1, 2, 3)
    obj_2 = MemoTestThreeParamsClass(1, 2, 3)
    db0.assign(obj_1, obj_2, value_1=4, value_2=5, value_3=6)
    for obj in (obj_1, obj_2):
        assert obj.value_1 == 4
        assert obj.value_2 == 5
        assert obj.value_3 == 6
    
    
def test_atomic_assign_multiple_attributes(db0_fixture):
    obj = MemoTestThreeParamsClass(1, 2, 3)
    db0.atomic_assign(obj, value_1=4, value_2=5, value_3=6)
    assert obj.value_1 == 4
    assert obj.value_2 == 5
    assert obj.value_3 == 6
    