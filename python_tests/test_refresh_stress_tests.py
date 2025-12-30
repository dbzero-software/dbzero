# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import multiprocessing
import time
import dbzero as db0
import os
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton
    

def append_to_prefix(prefix, obj_count = 50, commit_count = 50, long_run = False):
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
        if long_run:
            print(f"Writer process: committed {(i + 1) * obj_count} objects", flush=True)
        else:
            time.sleep(0.1)
    
    if long_run:
        print(db0.get_storage_stats(), flush=True)    
    db0.close()    
    

def validate_current_prefix(expected_len = None, expected_min_len = None):
    # refresh to assure we have latest data
    db0.refresh()
    # NOTE: reader process needs to use snapshots for concurrency safety
    with db0.snapshot() as snap:
        root = snap.fetch(MemoTestSingleton)
        print("--- begin iterate / validation", flush=True)
        assert not expected_min_len or len(root.value) >= expected_min_len
        assert not expected_len or len(root.value) == expected_len
        for item in root.value:
            assert item.value == "b" * 1024
        print(f"--- end iterate len = {len(root.value)}", flush=True)
        return len(root.value)


def rand_string(str_len):
    import random
    import string    
    return ''.join(random.choice(string.ascii_letters) for i in range(str_len))


def create_process_refresh_query_while_adding(px_name, num_iterations,
                                              num_objects, str_len):
    db0.init(DB0_DIR)
    db0.open(px_name, "rw")
    for _ in range(num_iterations):          
        for index in range(num_objects):
            obj = MemoTestClass(rand_string(str_len))
            db0.tags(obj).add("tag1")
            if index % 3 == 0:
                db0.tags(obj).add("tag2")            
        db0.commit()
    db0.close()


@pytest.mark.stress_test
def test_refresh_query_while_adding_new_objects(db0_fixture):
    px_name = db0.get_current_prefix().name
    
    db0.commit()
    db0.close()
    
    num_iterations = 1
    num_objects = 1000
    str_len = 4096
    p = multiprocessing.Process(target=create_process_refresh_query_while_adding, 
                                args = (px_name, num_iterations, num_objects, str_len))
    p.start()
    
    try:
        db0.init(DB0_DIR)
        db0.open(px_name, "r")
        while True:
            db0.refresh()
            time.sleep(0.1)
            query_len = len(list(db0.find(MemoTestClass, "tag1")))        
            print(f"Query length: {query_len}")
            if query_len == num_iterations * num_objects:
                break
    finally:
        p.terminate()
        p.join()
        db0.close()

@pytest.mark.skip(reason="https://github.com/dbzero-software/dbzero/issues/662")
@pytest.mark.stress_test
def test_continuous_refresh_process(db0_fixture):
    px_name = db0.get_current_prefix().name
    db0.close()
    
    # in each 'epoch' we modify prefix while making copies
    # then drop the original prefix and restore if from the last copy
    epoch_count = 2
    total_len = 0
    for epoch in range(epoch_count):
        print(f"=== Epoch {epoch} ===")
        obj_count = 5000
        commit_count = 100
        # start the writer process for a long run
        p = multiprocessing.Process(target=append_to_prefix, args=(px_name, obj_count, commit_count, True))
        p.start()
        
        db0.init(DB0_DIR)
        db0.open(px_name, "r")
        last_len = 0
        while True:
            # NOTE: reader needs to use snapshots for concurrency safety
            with db0.snapshot() as snap:
                if not snap.exists(MemoTestSingleton):
                    time.sleep(0.1)
                    continue
                root = snap.fetch(MemoTestSingleton)
                if len(root.value) > 1:
                    last_len = len(root.value)
                    break
            time.sleep(0.1)
        
        # validate prefix while writer is actively modifying it
        while True:        
            if not p.is_alive():
                break
            print("--- Validate  prefix iteration", flush=True)            
            last_len = validate_current_prefix(expected_min_len = last_len)
            print(f"--- Prefix valid with {last_len} objects", flush=True)
            if not p.is_alive():
                break
            time.sleep(0.25)
        
        p.terminate()
        p.join()
        total_len += obj_count * commit_count
        
        print("Validating final prefix ...", flush=True)         
        validate_current_prefix(expected_len = total_len)        
        db0.close()
