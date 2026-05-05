# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

"""
Regression test: segfault when db0.find() is called from inside a @db0.memo
object's __init__ after db0.tags(self).add() has been called on that object.

Minimal sequence:
  1. Any @db0.memo class TypeA exists
  2. A @db0.memo class TypeB calls db0.tags(self).add(tag) in __init__,
     then calls db0.find(TypeA, tag) in the same __init__
  3. Creating an instance of TypeB → segfault in db0.find

Root cause: db0.tags(self).add() partially registers the in-progress object
in the tag index; the subsequent db0.find() traverses that index and
dereferences the not-yet-committed object, causing a null-pointer crash.
"""

import gc
import os
import shutil

import pytest
import dbzero as db0

from .conftest import DB0_DIR


@db0.memo
class FindTarget:
    def __init__(self, value: int):
        self.value = value


@db0.memo
class TaggerAndFinder:
    def __init__(self):
        db0.tags(self).add("my-tag")
        self._results = list(db0.find(FindTarget, "my-tag"))


@pytest.fixture()
def db0_rw_fixture():
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR, read_write=True)
    db0.open("test_prefix", "rw")
    yield db0
    gc.collect()
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


def test_find_inside_memo_init_after_tagging_self(db0_rw_fixture):
    """db0.find() inside a memo __init__ must not segfault after db0.tags(self).add()."""
    obj = TaggerAndFinder()
    assert obj._results == []

def test_find_memo_init_after_tagging_in_init(db0_rw_fixture):
    """db0.find() inside a memo __init__ must not segfault after db0.tags(self).add()."""
    obj = TaggerAndFinder()
    obj2 = TaggerAndFinder()
    assert len(list(db0.find(TaggerAndFinder, "my-tag"))) == 2
