# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
import pytest
from .conftest import DB0_DIR
from .memo_test_types import MemoTestSingleton, MemoTestClass
import multiprocessing
import os
import time


def writer_process(prefix, obj_count = 50, commit_count = 50):
    db0.init(DB0_DIR)
    db0.open(prefix, "rw")
    # create new or open an existing root object
    root = MemoTestSingleton([])
    if (len(root.value) > 0):
        print(f"Writer process: opened existing prefix with {len(root.value)} objects")
    for i in range(commit_count):
        for _ in range(obj_count):
            root.value.append(MemoTestClass("b" * 1024))  # 1 KB string
        db0.commit()

    db0.commit()
    db0.close()


@pytest.mark.parametrize("db0_fixture", [{"autocommit": False}], indirect=True)
def test_copy_prefix_issue1(db0_fixture):
    """
    Issue: test failing with RuntimeError: Diff block not found
    """
    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")
    
    def validate_current_prefix(expected_len = None, expected_min_len = None):
        root = db0.fetch(MemoTestSingleton)
        assert not expected_min_len or len(root.value) > expected_min_len
        assert not expected_len or len(root.value) == expected_len        
        for item in root.value:
            assert item.value == "b" * 1024        
        return len(root.value)
    
    def validate_copy(copy_id, expected_len = None, expected_min_len = None):
        file_name = f"./test-copy-{copy_id}.db0"
        os.remove(px_path)
        # restore the copy
        os.rename(file_name, px_path)
                
        db0.init(DB0_DIR, prefix=px_name, read_write=False)
        result = validate_current_prefix(expected_len, expected_min_len)
        db0.close()
        return result
    
    db0.close()
    
    # in each 'epoch' we modify prefix while making copies
    # then drop the original prefix and restore if from the last copy
    epoch_count = 2
    total_len = 0
    for _ in range(epoch_count):
        obj_count = 30
        commit_count = 3
        writer_process(px_name, obj_count, commit_count)
        total_len += obj_count * commit_count

        db0.init(DB0_DIR)
        db0.open(px_name, "r")
        
        # make final stale copy (i.e. without active modifications)
        final_copy = f"./test-copy-final.db0"
        if os.path.exists(final_copy):
            os.remove(final_copy)
        db0.copy_prefix(final_copy, prefix=px_name)    
        db0.close()
                
        validate_copy("final", expected_len = total_len)
