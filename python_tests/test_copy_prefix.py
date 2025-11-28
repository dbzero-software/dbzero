import dbzero as db0
import os
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


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
