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


def writer_process(prefix):
    db0.init(DB0_DIR)
    db0.open(prefix, "rw")
    root = MemoTestSingleton([])
    for _ in range(50):
        for _ in range(50):
            root.value.append(MemoTestClass("b" * 1024))  # 1 KB string
        db0.commit()
        time.sleep(0.1)
    
    db0.commit()
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
