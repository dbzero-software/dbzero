# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from .conftest import DB0_DIR
import shutil
import os


def test_cache_size_can_be_specified_on_init(db0_fixture):
    db0.close()
    db0.init(DB0_DIR, cache_size=123456, lang_cache_size=9876)
    db0.open("my-test-prefix")    
    assert db0.get_lang_cache_stats()["capacity"] == 9876

    stats = db0.get_cache_stats()
    assert stats["capacity"] == 123456
