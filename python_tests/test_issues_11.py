import dbzero as db0
import pytest
from .conftest import DB0_DIR
from datetime import datetime
from .memo_test_types import MemoBlob
import random


@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 64 << 20, "autocommit": False}], indirect=True)
def test_wide_lock_badaddr_issue(db0_slab_size):
    db0.set_cache_size(8 << 30)
    all_blobs = db0.list()
    append_count = 300289
    # append large blobs to force wide locks
    for count in range(append_count):
        all_blobs.append(MemoBlob(24 << 10))
        if count % 50000 == 0:
            print("Commit...")
            db0.commit()
            print(f"Appended {count} blobs")
    
    # faulty operation
    all_blobs.append(MemoBlob(24 << 10))
            