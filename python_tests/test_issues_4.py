# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from unittest.mock import patch


@db0.memo
class PatchedClass:
    def __init__(self, value):
        self.__value = value
        
    def get_value(self):
        return self.__value
    
    
def test_memo_class_can_be_patched_issue(db0_fixture):
    """
    Issue description: the test was initially failing with AttributeError: get_value not found
    """
    original = PatchedClass(42)
    assert original.get_value() == 42

    with patch.object(PatchedClass, 'get_value', return_value=None):
        patched = PatchedClass(42)
        assert patched.get_value() is None
