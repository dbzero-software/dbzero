# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from .conftest import MemoTestSingleton
from .conftest import DB0_DIR


def test_memo_class_persistence_issue1(db0_fixture):
    px_name = db0.get_current_prefix().name
    obj = MemoTestSingleton(1)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    assert MemoTestSingleton().value == 1
    