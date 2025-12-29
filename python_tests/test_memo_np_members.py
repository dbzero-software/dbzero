# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from dataclasses import dataclass
from threading import Lock
from dataclasses import field, dataclass


@db0.memo
@dataclass
class MemoWithNPMember:
    count: int = 0
    _X__lock: Lock = field(default_factory=Lock, repr=False, compare=False)
    
    @property
    def lock(self):
        return self._X__lock


@db0.memo
@dataclass
class DerivedMemoWithNPMember(MemoWithNPMember):
    value: str = "default"
    
    
def test_memo_instance_with_non_persistent_members_can_be_created(db0_fixture):
    obj_1 = MemoWithNPMember(10)
    obj_2 = MemoWithNPMember(10)
    assert obj_1.lock is not None
    assert obj_1.lock is not obj_2.lock
    
    
def test_derived_memo_instance_with_non_persistent_members_can_be_created(db0_fixture):
    obj_1 = DerivedMemoWithNPMember(count = 5, value="test")    
    assert obj_1.lock is not None
    assert obj_1.count == 5
    assert obj_1.value == "test"
