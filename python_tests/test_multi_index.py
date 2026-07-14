# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoScopedClass, MemoScopedSingleton
from dbzero import find


def test_select_from_multiple_indexes(db0_fixture):
    keys_1 = [0, 1, 2, 3, 4, 5, 6, 7, 8, None]
    keys_2 = [0, 0, 0, 1, 1, 1, 1, 2, 2, 1]
    objects = [MemoTestClass((k,v)) for k,v in zip(keys_1, keys_2)]
    ix1, ix2 = (db0.index(), db0.index())
    for obj, k1, k2 in zip(objects, keys_1, keys_2):
        ix1.add(k1, obj)
        ix2.add(k2, obj)
    
    query = (
        ix1.select(low=None, high=5, null_first=True),
        ix2.select(low=1, high=None, null_first=True)
    )

    assert len(list(db0.find(query))) == 4
    
            
def test_sort_results_from_multiple_indexes(db0_fixture):    
    keys_1 = [0, 1, 2, 3, 4, 5, 6, 7, 8, None]
    keys_2 = [0, 0, 0, 1, 3, 1, 1, 2, 2, 2]
    objects = [MemoTestClass((k,v)) for k,v in zip(keys_1, keys_2)]
    ix1, ix2 = (db0.index(), db0.index())
    for obj, k1, k2 in zip(objects, keys_1, keys_2):
        ix1.add(k1, obj)
        ix2.add(k2, obj)
    
    query = (
        ix1.select(low=None, high=5, null_first=True),
        ix2.select(low=1, high=None, null_first=True)
    )
    
    # sort using index 1
    sorted = db0.list(ix1.sort(db0.find(query), null_first=False))
    assert [x.value[0] for x in sorted] == [3, 4, 5, None]
    
    sorted = db0.list(ix1.sort(db0.find(query), null_first=True))
    assert [x.value[0] for x in sorted] == [None, 3, 4, 5]
    
    # sort using index 2
    sorted = db0.list(ix2.sort(db0.find(query), null_first=False))
    assert [x.value[1] for x in sorted] == [1, 1, 2, 3]
    
            
def test_sort_with_unrelated_index(db0_fixture):
    keys_1 = [0, 1, 2, 3, 4, 5, 6, 7, 8, None]
    keys_2 = [0, 0, 0, 1, 3, 1, 1, 2, 2, 2]
    keys_3 = [0, 0, 0, None, 4, 3, 2, 1, 0, 5]
    objects = [MemoTestClass((k,v)) for k,v in zip(keys_1, keys_2)]
    ix1, ix2, ix3 = (db0.index(), db0.index(), db0.index())
    for obj, k1, k2, k3 in zip(objects, keys_1, keys_2, keys_3):
        ix1.add(k1, obj)
        ix2.add(k2, obj)
        ix3.add(k3, obj)
    
    query = (
        ix1.select(low=None, high=5, null_first=True),
        ix2.select(low=1, high=None, null_first=True)
    )
    
    # sort using index 3
    sorted = db0.list(ix3.sort(db0.find(query), null_first=True))
    assert [x.value[0] for x in sorted] == [3, 5, 4, None]
