# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from .memo_test_types import MemoScopedClass

def test_assing_bool_to_memo(db0_fixture):
    m = MemoScopedClass(True)
    assert m.value == True
    m = MemoScopedClass(False)
    assert m.value == False