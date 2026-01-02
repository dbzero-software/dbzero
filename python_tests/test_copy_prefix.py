# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
import os
import time
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR
import multiprocessing


def test_copy_current_prefix(db0_fixture):
    file_name = "./test-copy.db0"
    # remove file if it exists
    if os.path.exists(file_name):
        os.remove(file_name)
    
    root = MemoTestSingleton([])
    for _ in range(50):
        root.value.append(MemoTestClass("a" * 1024))  # 1 KB string
    db0.commit()
    
    # NOTE: db0 must be initialized to copy existing prefix
    db0.copy_prefix(file_name)
    # make sure file exists
    assert os.path.exists(file_name)
    # drop copy
    os.remove(file_name)    


def test_recover_prefix_from_copy(db0_fixture):
    file_name = "./test-copy.db0"
    # remove file if it exists
    if os.path.exists(file_name):
        os.remove(file_name)

    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")
    
    root = MemoTestSingleton([])
    for _ in range(50):
        root.value.append(MemoTestClass("a" * 1024))  # 1 KB string
    db0.commit()        
    db0.copy_prefix(file_name)
    db0.close()
    
    # drop original file and replace with copy
    os.remove(px_path)
    os.rename(file_name, px_path)
    
    # open dbzero and read all data
    db0.init(DB0_DIR, prefix=px_name, read_write=False)
    root = db0.fetch(MemoTestSingleton)
    for item in root.value:
        assert item.value == "a" * 1024
    assert len(root.value) == 50


def test_copy_prefix_custom_step_size(db0_fixture):
    file_name = "./test-copy.db0"
    if os.path.exists(file_name):
        os.remove(file_name)
    
    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")
    
    root = MemoTestSingleton([])
    for count in range(500):
        root.value.append(MemoTestClass("b" * 1024))  # 1 KB string
        if (count % 50) == 0:
            db0.commit()
    db0.commit()
    
    # copy using custom (small) step size (64 KB)
    db0.copy_prefix(file_name, page_io_step_size=64 << 10)
    db0.close()
    
    # drop original file and replace with copy
    os.remove(px_path)
    os.rename(file_name, px_path)
    
    # open dbzero and read all data
    db0.init(DB0_DIR, prefix=px_name, read_write=False)
    root = db0.fetch(MemoTestSingleton)
    for item in root.value:
        assert item.value == "b" * 1024
    assert len(root.value) == 500


def writer_process(prefix, obj_count = 50, commit_count = 50, long_run = False):
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


def test_copy_prefix_being_actively_modified(db0_fixture):
    file_name = "./test-copy.db0"
    if os.path.exists(file_name):
        os.remove(file_name)
    
    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")
    
    db0.close()
    # start the writer process and wait until some data is alreasdy there
    p = multiprocessing.Process(target=writer_process, args=(px_name,))
    p.start()
    
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    while True:
        try:
            root = db0.fetch(MemoTestSingleton)
            if len(root.value) > 150:
                break
        except Exception:
            pass
        time.sleep(0.1)
    
    # copy the prefix while it is being modified
    db0.copy_prefix(file_name, page_io_step_size=64 << 10)
    db0.close()
    
    p.terminate()
    p.join()
    
    # drop original file and replace with copy
    os.remove(px_path)
    os.rename(file_name, px_path)
    
    # open dbzero and read all data
    db0.init(DB0_DIR, prefix=px_name, read_write=False)
    root = db0.fetch(MemoTestSingleton)
    for item in root.value:
        assert item.value == "b" * 1024
    assert len(root.value) >= 150


def test_copy_prefix_fails_if_no_active_prefix(db0_fixture):
    file_name = "./test-copy.db0"
    # remove file if it exists
    if os.path.exists(file_name):
        os.remove(file_name)
    
    px_name = db0.get_current_prefix().name
    root = MemoTestSingleton([])
    for _ in range(5):
        root.value.append(MemoTestClass("a" * 1024))  # 1 KB string
    db0.commit()
    db0.close()
    
    # init dbzero without opening the prefix
    db0.init(DB0_DIR)
    with pytest.raises(RuntimeError):    
        db0.copy_prefix(file_name)


def test_copy_prefix_without_opening_it(db0_fixture):
    file_name = "./test-copy.db0"
    # remove file if it exists
    if os.path.exists(file_name):
        os.remove(file_name)

    px_name = db0.get_current_prefix().name
    root = MemoTestSingleton([])
    for _ in range(5):
        root.value.append(MemoTestClass("a" * 1024))  # 1 KB string
    db0.commit()
    db0.close()
    
    # init dbzero without opening the prefix
    db0.init(DB0_DIR)    
    # copy existing prefix without opening it
    db0.copy_prefix(file_name, prefix = px_name)
    assert os.path.exists(file_name)
    os.remove(file_name)    


@pytest.mark.stress_test
@pytest.mark.skip(reason="https://github.com/dbzero-software/dbzero/issues/662")
def test_copy_prefix_continuous_process(db0_fixture):
    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")

    def validate_current_prefix(expected_len = None, expected_min_len = None):
        root = db0.fetch(MemoTestSingleton)
        assert not expected_min_len or len(root.value) >= expected_min_len
        assert not expected_len or len(root.value) == expected_len        
        for item in root.value:
            assert item.value == "b" * 1024        
        return len(root.value)

    def validate_copy(copy_id, expected_len = None, expected_min_len = None):
        file_name = f"./test-copy-{copy_id}.db0"
        os.remove(px_path)
        # restore the copy
        os.rename(file_name, px_path)
        
        print(f"--- Validating copy {copy_id}", flush=True)
        db0.init(DB0_DIR, prefix=px_name, read_write=False)
        result = validate_current_prefix(expected_len, expected_min_len)
        db0.close()
        return result
    
    db0.close()
    
    # in each 'epoch' we modify prefix while making copies
    # then drop the original prefix and restore if from the last copy    
    epoch_count = 3
    total_len = 0
    for epoch in range(epoch_count):
        print(f"=== Epoch {epoch} ===", flush=True)
        obj_count = 5000
        commit_count = 100
        # start the writer process for a long run
        p = multiprocessing.Process(target=writer_process, args=(px_name, obj_count, commit_count, True))
        p.start()
        
        db0.init(DB0_DIR)
        db0.open(px_name, "r")
        last_len = 0
        while True:
            try:
                root = db0.fetch(MemoTestSingleton)
                if len(root.value) > 1:
                    last_len = len(root.value)
                    break
            except Exception:
                pass
            time.sleep(0.1)
        
        copy_id = 0
        # copy the prefix multiple times while it is being modified 
        while True:
            if not p.is_alive():
                break
            file_name = f"./test-copy-{copy_id}.db0"
            if os.path.exists(file_name):
                os.remove(file_name)
            # copy prefix without opening it, use default step size
            print("--- Copying prefix iteration", copy_id, flush=True)
            db0.copy_prefix(file_name, prefix=px_name)
            print("--- copy finished", flush=True)
            copy_id += 1
            if not p.is_alive():
                break
            time.sleep(2.5)  # wait a bit before next copy
        
        p.join()
        total_len += obj_count * commit_count
                
        # make final stale copy (i.e. without active modifications)
        final_copy = f"./test-copy-final.db0"
        if os.path.exists(final_copy):
            os.remove(final_copy)
        db0.copy_prefix(final_copy, prefix=px_name)    
        db0.close()
        
        print("Validating all copies", flush=True)
        validate_copy("final", expected_len = total_len)
        for i in range(copy_id):
            last_len = validate_copy(i, expected_min_len = last_len)
            print(f"--- Copy {i} valid with {last_len} objects", flush=True)
            # this is the restored version
            total_len = last_len
        
        # now, continue modifications starting from the last restored copy (making new copies)


def test_modify_copied_prefix(db0_fixture):
    file_name = "./test-copy.db0"
    # remove file if it exists
    if os.path.exists(file_name):
        os.remove(file_name)

    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")
    root = MemoTestSingleton([])
    total_len = 0
    
    def modify_prefix():
        append_count = 0
        root = db0.fetch(MemoTestSingleton)
        for _ in range(50):
            root.value.append(MemoTestClass("a" * 1024))  # 1 KB string
            append_count += 1
        db0.commit()
        return append_count
    
    total_len += modify_prefix()
    db0.copy_prefix(file_name)
    db0.close()
    
    # drop original file and replace with copy
    os.remove(px_path)
    os.rename(file_name, px_path)
    
    # open recovered prefix for update
    db0.init(DB0_DIR, prefix=px_name, read_write=True)
    total_len += modify_prefix()
    db0.close()

    # open prefix from recovered and modified copy
    db0.init(DB0_DIR, prefix=px_name, read_write=False)
    root = db0.fetch(MemoTestSingleton)
    for item in root.value:
        assert item.value == "a" * 1024
    assert len(root.value) == total_len


@pytest.mark.parametrize("db0_fixture", [{"autocommit": False}], indirect=True)
def test_copy_prefix_of_recovered_copy(db0_fixture):
    file_name = "./test-copy.db0"
    # remove file if it exists
    if os.path.exists(file_name):
        os.remove(file_name)
    
    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")
    root = MemoTestSingleton([])
    total_len = 0
    charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    
    def modify_prefix(op_count = 50):
        append_count = 0
        root = db0.fetch(MemoTestSingleton)
        for _ in range(op_count):
            c = charset[len(root.value) % len(charset)]
            root.value.append(MemoTestClass(c * 1024))  # 1 KB string
            append_count += 1
        db0.commit()
        return append_count

    def validate(expected_len):
        root = db0.fetch(MemoTestSingleton)
        for i, item in enumerate(root.value):
            c = charset[i % len(charset)]
            assert item.value == c * 1024
        assert len(root.value) == expected_len
    
    total_len += modify_prefix(5150)
    db0.copy_prefix(file_name, page_io_step_size=64 << 10)
    db0.close()

    # drop original file and replace with copy
    os.remove(px_path)
    os.rename(file_name, px_path)
    
    # open recovered prefix for update
    db0.init(DB0_DIR, prefix=px_name, read_write=True)    
    total_len += modify_prefix(1350)

    db0.close()
    db0.init(DB0_DIR, prefix=px_name, read_write=True)
    validate(total_len)
    db0.copy_prefix(file_name)
    db0.close()
    
    # restore copy of a restored and modified copy
    os.remove(px_path)
    os.rename(file_name, px_path)
    
    # open prefix from recovered and modified copy of a copy
    db0.init(DB0_DIR, prefix=px_name, read_write=False)
    validate(total_len)
    
    
def test_slow_copy(db0_fixture):
    """
    Test simulating fast writer and slow copy/reader process (debug mode only)
    """
    if 'D' in db0.build_flags():        
        px_name = db0.get_current_prefix().name
        px_path = os.path.join(DB0_DIR, px_name + ".db0")

        def validate_current_prefix(expected_len = None, expected_min_len = None):
            root = db0.fetch(MemoTestSingleton)
            assert not expected_min_len or len(root.value) >= expected_min_len
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
        
        obj_count = 250
        commit_count = 15
        # start the writer process for a long run
        p = multiprocessing.Process(target=writer_process, args=(px_name, obj_count, commit_count, True))
        p.start()
        
        db0.init(DB0_DIR)
        db0.open(px_name, "r")
        last_len = 0
        while True:
            try:
                if not db0.exists(MemoTestSingleton):
                    time.sleep(0.1)
                    continue
                root = db0.fetch(MemoTestSingleton)
                if len(root.value) > 1:
                    last_len = len(root.value)
                    break
            except Exception:
                pass
            time.sleep(0.1)
        
        copy_id = 0
        # copy the prefix multiple times while it is being modified 
        db0.set_test_params(sleep_interval = 50)
        while True:
            if not p.is_alive():
                break
            file_name = f"./test-copy-{copy_id}.db0"
            if os.path.exists(file_name):
                os.remove(file_name)
            db0.copy_prefix(file_name, prefix=px_name)
            copy_id += 1
            if not p.is_alive():
                break            
        
        p.join()
        db0.close()
        
        for i in range(copy_id):
            last_len = validate_copy(i, expected_min_len = last_len)            
    

@pytest.mark.stress_test
@pytest.mark.skip(reason="https://github.com/dbzero-software/dbzero/issues/662")
def test_copy_prefix_continuous_process_slow_copy(db0_fixture):
    if 'D' in db0.build_flags(): 
        px_name = db0.get_current_prefix().name
        px_path = os.path.join(DB0_DIR, px_name + ".db0")
        db0.set_test_params(sleep_interval = 50)
        def validate_current_prefix(expected_len = None, expected_min_len = None):
            root = db0.fetch(MemoTestSingleton)
            assert not expected_min_len or len(root.value) >= expected_min_len
            assert not expected_len or len(root.value) == expected_len        
            for item in root.value:
                assert item.value == "b" * 1024        
            return len(root.value)

        def validate_copy(copy_id, expected_len = None, expected_min_len = None):
            file_name = f"./test-copy-{copy_id}.db0"
            os.remove(px_path)
            # restore the copy
            os.rename(file_name, px_path)
            
            print(f"--- Validating copy {copy_id}", flush=True)
            db0.init(DB0_DIR, prefix=px_name, read_write=False)
            result = validate_current_prefix(expected_len, expected_min_len)
            db0.close()
            return result
        
        db0.close()
        
        # in each 'epoch' we modify prefix while making copies
        # then drop the original prefix and restore if from the last copy    
        epoch_count = 3
        total_len = 0
        for epoch in range(epoch_count):
            print(f"=== Epoch {epoch} ===", flush=True)
            obj_count = 500
            commit_count = 100
            # start the writer process for a long run
            p = multiprocessing.Process(target=writer_process, args=(px_name, obj_count, commit_count, True))
            p.start()
            
            db0.init(DB0_DIR)
            db0.open(px_name, "r")
            last_len = 0
            time.sleep(3)
            while True:
                try:
                    root = db0.fetch(MemoTestSingleton)
                    if len(root.value) > 1:
                        last_len = len(root.value)
                        break
                except Exception as ex:
                    print("Exception while fetching root:", ex, flush=True)
                    pass
                time.sleep(0.1)
            
            copy_id = 0
            # copy the prefix multiple times while it is being modified 
            while True:
                if not p.is_alive():
                    break
                file_name = f"./test-copy-{copy_id}.db0"
                if os.path.exists(file_name):
                    os.remove(file_name)
                # copy prefix without opening it, use default step size
                print("--- Copying prefix iteration", copy_id, flush=True)
                db0.copy_prefix(file_name, prefix=px_name)
                print("--- copy finished", flush=True)
                copy_id += 1
                if not p.is_alive():
                    break
                time.sleep(2.5)  # wait a bit before next copy
            
            p.join()
            total_len += obj_count * commit_count
                    
            # make final stale copy (i.e. without active modifications)
            final_copy = f"./test-copy-final.db0"
            if os.path.exists(final_copy):
                os.remove(final_copy)
            db0.copy_prefix(final_copy, prefix=px_name)    
            db0.close()
            
            print("Validating all copies", flush=True)
            validate_copy("final", expected_len = total_len)
            for i in range(copy_id):
                last_len = validate_copy(i, expected_min_len = last_len)
                print(f"--- Copy {i} valid with {last_len} objects", flush=True)
                # this is the restored version
                total_len = last_len
            
   