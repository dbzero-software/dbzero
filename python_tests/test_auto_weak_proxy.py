# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestPxClass


# --- auto_weak_proxy is always active ---

def test_auto_wrap_on_cross_prefix_assignment(db0_fixture):
    """Cross-prefix assignment is silently wrapped as weak_proxy by default."""
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    # should NOT raise - auto-wrap replaces the old exception
    obj_2 = MemoTestPxClass(obj_1, prefix=px_2)
    assert obj_2 is not None


def test_auto_wrap_value_is_accessible(db0_fixture):
    """Auto-wrapped cross-prefix value can be read back."""
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass(obj_1, prefix=px_2)
    assert obj_2.value.value == 123


def test_auto_wrap_does_not_increment_refcount(db0_fixture):
    """Auto-wrapped weak ref does not increment the source object's ref count."""
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    ref_before = db0.getrefcount(obj_1)
    MemoTestPxClass(obj_1, prefix=px_2)
    assert db0.getrefcount(obj_1) == ref_before


def test_auto_wrap_expires_when_source_deleted(db0_fixture):
    """Auto-wrapped ref expires when the original object is deleted (weak ref semantics)."""
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass(obj_1, prefix=px_2)
    del obj_1
    db0.commit()
    with pytest.raises(db0.ReferenceError):
        _ = obj_2.value.value



def test_auto_wrap_on_cross_prefix_assignment(db0_fixture):
    """Cross-prefix assignment is silently wrapped as weak_proxy by default."""
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass([1,2,3], prefix=px_1)
    # should NOT raise - auto-wrap replaces the old exception
    obj_dict = MemoTestPxClass({}, prefix=px_2)
    obj_dict.value["ref_to_obj_1"] = obj_1
    obj_dict.value["ref_to_obj_2"] = obj_2
    obj_dict.value["ref_to_obj_3"] = obj_2.value[:]
    value = obj_dict.value["ref_to_obj_1"]

    assert obj_dict.value["ref_to_obj_1"].value == 123
    assert obj_dict.value["ref_to_obj_2"].value == [1,2,3]
    for key, obj in obj_dict.value.items():
        if key == "ref_to_obj_1":
            assert obj.value == 123
        elif key == "ref_to_obj_2":
            assert obj.value == [1,2,3]
        elif key == "ref_to_obj_3":
            assert obj == [1,2,3]