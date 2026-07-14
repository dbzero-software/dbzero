# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass
    

def rand_string(str_len):
    import random
    import string    
    return ''.join(random.choice(string.ascii_letters) for i in range(str_len))

    
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 4 * 1024 * 1024}], indirect=True)
def test_collecting_slab_metrics(db0_slab_size):
    buf = []
    str_size = [18, 1273, 133, 912, 993, 9213]
    for str_len in str_size:
        buf.append(MemoTestClass(rand_string(str_len)))
    
    slab_metrics = db0.get_slab_metrics()
    # 2 slabs is user space (1 slab per realm), the other 2 are LSP & TYPE reserved slabs
    assert len(slab_metrics) == 4


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 4 * 1024 * 1024}], indirect=True)
def test_remaining_capacity_metric(db0_slab_size):
    buf = []    
    # before first measure, allocate an object to initialize slabs from all realms
    MemoTestClass(rand_string(32))
    trc1 = sum(slab["remaining_capacity"] for slab in db0.get_slab_metrics().values())
    str_size = [18, 1273, 133, 912, 993, 9213, 12312, 9432, 7349, 333]
    for str_len in str_size:
        buf.append(MemoTestClass(rand_string(str_len)))
    
    trc2 = sum(slab["remaining_capacity"] for slab in db0.get_slab_metrics().values())
    assert sum(str_size) / (trc1 - trc2) > 0.8
