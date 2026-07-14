# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import multiprocessing
import time
import dbzero as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton


def test_open_with_meta_io(db0_metaio_fixture):
    # create singleton with a list type member
    root = MemoTestSingleton([])
    prefix_name = db0.get_prefix_of(root).name
    
    steps = 10
    step_size = 100

    for _ in range(steps):
        for _ in range(step_size):
            root.value.append(MemoTestClass(123))
        db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")
    root = MemoTestSingleton()
    assert len(root.value) == steps * step_size


# update object from a separate process
def update_process(prefix_name, steps, step_size):
    time.sleep(0.1)
    db0.init(DB0_DIR)        
    # NOTE: we use very small meta_io_step_size to force many appends
    db0.open(prefix_name, "rw", meta_io_step_size = 16)
    root = MemoTestSingleton()
    for _ in range(steps):
        for _ in range(step_size):
            root.value.append(MemoTestClass(123))
        db0.commit()
        time.sleep(0.1)
    db0.close()

def test_refresh_with_meta_io_updates(db0_metaio_fixture):
    # create singleton with a list type member
    root = MemoTestSingleton([])
    prefix_name = db0.get_prefix_of(root).name
    
    steps = 10
    step_size = 100
    
    
    
    # close db0 and open as read-only
    db0.commit()
    db0.close()

    p = multiprocessing.Process(target=update_process, args=(prefix_name, steps, step_size))
    p.start()
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")    
    root = MemoTestSingleton()
    while len(root.value) < steps * step_size:        
        time.sleep(0.1)
    
    p.terminate()
    p.join()
    assert len(root.value) == steps * step_size    
