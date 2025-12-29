# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton


def test_raises_when_trying_to_link_different_prefixes(db0_fixture):
    object_1 = MemoTestClass(123)
    # construct a new object on a different prefix
    db0.open("some-other-prefix")
    object_2 = MemoTestClass(456)
    with pytest.raises(Exception):
        # this should raise since objects from different prefixes cannot be linked
        object_1.value = object_2
