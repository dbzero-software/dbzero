# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


def test_create_prefix_with_page_io_step_size(db0_fixture):
    # use 16 MB page I/O step size
    db0.open("some-new-prefix", "rw", page_io_step_size = 16 << 20)
    buf = []
    for _ in range(50):
        buf.append(MemoTestClass("a" * 1024))  # 1 KB string
        # commit after each append
        db0.commit()
    
    px_size_1 = db0.get_storage_stats()["prefix_size"]
    assert px_size_1 > (16 << 20)
    
    # SparsePairManager stores application sparse pairs in descriptor-backed meta-space.
    # DRAM metadata is append-only so concurrent readers can still open the previous
    # committed root state while a writer is publishing the next one. The file may
    # grow with metadata, but it must not allocate another 16 MB page-IO step.
    for _ in range(50):
        buf.append(MemoTestClass("a" * 1024))  # 1 KB string
        # commit after each append
        db0.commit()

    px_size_2 = db0.get_storage_stats()["prefix_size"]
    assert (px_size_2 - px_size_1) < (8 << 20)
    
    
def test_continue_append_with_step_size(db0_fixture):
    db0.open("some-new-prefix", "rw", page_io_step_size = 16 << 20)
    root = MemoTestSingleton([])    
    for _ in range(50):
        root.value.append(MemoTestClass("a" * 1024))  # 1 KB string
    db0.commit()
        
    db0.close()
    db0.init(DB0_DIR)
    # NOTE: we're opening an existing prefix with already initialized page I/O step size
    db0.open("some-new-prefix", "rw")
    root = db0.fetch(MemoTestSingleton)
    for _ in range(250):
        root.value.append(MemoTestClass("a" * 1024))
        db0.commit()
    
    px_size = db0.get_storage_stats()["prefix_size"]
    assert px_size > (16 << 20)
    assert px_size < (48 << 20)
    
