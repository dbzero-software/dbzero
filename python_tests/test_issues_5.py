# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass
from unittest.mock import patch


@db0.memo
class MemberCycleFoundation():
    def __init__(self):
        self.field_0 = 1
    

@db0.memo
class MemberCycleBaseClass(MemberCycleFoundation):
    def __init__(self):
        super().__init__()
        self.field_1 = 2
        self.field_2 = 3
        self.my_dict = {}
        self.field_6 = 7
        self.field_7 = 8
        self.cycle = MemoTestClass(self)
        for k, v in {"a": 1, "b": 2}.items():
            self.my_dict[k] = v
    

@db0.memo
class MemberCycleSubClass(MemberCycleBaseClass):
    def __init__(self, value):
        super().__init__()
        self._value = value
    
    
def test_reference_cycle_member_issue_1(db0_fixture):
    """
    Issue description: The test was failing with AttributeError: 'dbzero.Memo_PrivMemberSubClass' object has no attribute 'my_dict'
    when a cycle was created in the object graph.    
    """
    obj = MemberCycleSubClass(42)
    assert obj is not None