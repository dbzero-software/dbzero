# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from .memo_test_types import MemoTestClass,TriColor


def test_db0_can_open_long_prefix_name(db0_fixture):
    prefix_name = "/org/project/dev/some-long-prefix-name"
    db0.open(prefix_name)
    db0.drop(prefix_name)
