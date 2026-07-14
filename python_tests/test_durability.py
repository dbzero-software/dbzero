# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR
import multiprocessing
import random
import string 


def test_persisting_single_transaction_data(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    root = MemoTestSingleton([])
    # add objects to root
    for i in range(100):
        root.value.append(MemoTestClass(i))
    # drop from python (but no from db0)
    del root
    # now, close db0 and then reopen (transaction performed on close)
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    root = MemoTestSingleton()
    # check if the objects are still there
    for i in range(100):
        assert root.value[i].value == i


def test_persisting_data_in_multiple_transactions(db0_fixture):
    prefix = db0.get_current_prefix()
    root = MemoTestSingleton([])
    # add objects to root (in 10 transactions)
    for i in range(10):
        for j in range(10):
            root.value.append(MemoTestClass(i * 10 + j))
        db0.commit()
    # drop from python (but no from db0)
    del root
    # now, close db0 and then reopen (transaction performed on close)
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix.name)
    root = MemoTestSingleton()
    # check if the objects are still there
    num = 0
    for obj in root.value:
        assert obj.value == num
        num += 1


def test_persisting_data_in_multiple_independent_transactions(db0_fixture):
    prefix = db0.get_current_prefix()
    root = MemoTestSingleton([])
    # add objects to root (in 10 transactions)
    # close and reopen db0 after each transaction
    for i in range(10):
        for j in range(10):
            root.value.append(MemoTestClass(i * 10 + j))
        db0.commit()
        db0.close()
        db0.init(DB0_DIR)
        db0.open(prefix.name)
        # need to reopen since python object is no longer accessible after close
        root = MemoTestSingleton()
    
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix.name)
    root = MemoTestSingleton()    
    num = 0
    for obj in root.value:
        assert obj.value == num
        num += 1

def open_prefix_then_crash():
    db0.open("new-prefix-1")
    db0.tags(MemoTestClass(123)).add("tag1", "tag2")
    # end process with exception before commit / close
    raise Exception("Crash!")


def test_opening_prefix_of_crashed_process(db0_no_default_fixture):
    p = multiprocessing.Process(target=open_prefix_then_crash)
    p.start()
    p.join()
    
    # try opeining the crashed prefix for read
    db0.open("new-prefix-1", "rw")
    assert len(list(db0.find("tag1"))) == 0
    

def test_modify_prefix_of_crashed_process(db0_no_default_fixture):
    p = multiprocessing.Process(target=open_prefix_then_crash)
    p.start()
    p.join()
    
    # try opeining the crashed prefix for read/write and append some objects
    db0.open("new-prefix-1", "rw")
    db0.tags(MemoTestClass(123)).add("tag1", "tag2")    
    db0.commit()


def rand_string(max_len):
    import random
    import string
    actual_len = random.randint(1, max_len)
    return ''.join(random.choice(string.ascii_letters) for i in range(actual_len))    


def create_objects(append_count=1000):
    db0.open("new-prefix-1")        
    buf = db0.list()
    root = MemoTestSingleton(buf)        
    transaction_bytes = 0
    commit_bytes = 512 * 1024
    for _ in range(append_count):
        buf.append(MemoTestClass(rand_string(8192)))
        transaction_bytes += len(buf[-1].value)
        if transaction_bytes > commit_bytes:                
            db0.commit()
            transaction_bytes = 0
    db0.commit()
    db0.close()


def test_dump_dram_io_map(db0_fixture):
    if 'D' in db0.build_flags():
        io_map = db0.get_dram_io_map()        
        assert len(io_map) > 0
    

def test_transactions_issue1(db0_no_autocommit):
    """
    Test was failing with:  Assertion `unload_member_functions[static_cast<int>(storage_class)]' failed
    Resolution: missing implementation of v_bvector::commit
    """
    buf = db0.list()
    for _ in range(6):
        for _ in range(50):
            buf.append(0)            
        db0.commit()
    
    index = 0
    for index in range(len(buf)):        
        assert buf[index] == 0
        index += 1


def test_low_cache_transactions_issue1(db0_no_autocommit):
    """
    Test was failing with: element mismatch (when running with very small cache size)
    Resolution: PrefixCache::findBoundaryRange must NOT create r/w boundary lock when lhs/rhs states don't match
    """
    def rand_string(str_len):
        return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
    
    db0.set_cache_size(64 << 10)
    buf = db0.list()
    py_buf = []
    for _ in range(2):
        for _ in range(34):
            str = rand_string(64)            
            buf.append(str)
            py_buf.append(str)
        db0.commit()
    
    index = 0
    for index in range(len(buf)):
        assert buf[index] == py_buf[index]
        index += 1
    
    
def test_low_cache_transactions_issue2(db0_no_autocommit):
    """
    Test was failing with: CRDT_Allocator.cpp, line 157: Invalid address: NNN    
    Resolution: expired lock eviction policy fixed (higher state number must be retained in the Page Map)    
    """
    def rand_string(str_len):
        return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
    
    db0.set_cache_size(100 << 10)
    buf = db0.list()
    py_buf = []
    for _ in range(2):
        for _ in range(17):
            str = rand_string(33)
            buf.append(MemoTestClass(str))
            py_buf.append(str)
        db0.commit()

    index = 0
    for index in range(len(buf)):
        assert buf[index].value == py_buf[index]        
        index += 1


def test_low_cache_transactions_issue3(db0_no_autocommit):
    """
    Test was failing with: CRDT_Allocator.cpp, line 157: Invalid address: NNN
    Resolution: DirtyCache::flush operation was flushing locks owned by language types causing multiple flush with no reads
    """
    def rand_string(str_len):
        return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
    
    db0.set_cache_size(100 << 10)
    buf = db0.list()
    py_buf = []
    for _ in range(8):
        for _ in range(28):
            str = rand_string(64)
            buf.append(MemoTestClass(str))
            py_buf.append(str)
        db0.commit()
    
    index = 0
    for index in range(len(buf)):
        assert buf[index].value == py_buf[index]        
        index += 1


def test_low_cache_transactions_issue4(db0_no_autocommit):
    """
    Test was failing with: failed asserd on sgb_tree_node.hpp:109: (index < m_size)
    Resolution: added missing fflush between fwrite / fread operations
    """
    def rand_string(str_len):
        return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
    
    db0.set_cache_size(100 << 10)
    buf = db0.list()
    py_buf = []
    for _ in range(83):
        for _ in range(200):
            str = rand_string(64)
            buf.append(MemoTestClass(str))
            py_buf.append(str)
        db0.commit()
    
    index = 0
    for index in range(len(buf)):
        assert buf[index].value == py_buf[index]        
        index += 1


@pytest.mark.parametrize("db0_autocommit_fixture", [1], indirect=True)
def test_low_cache_mt_issue1(db0_autocommit_fixture):
    """
    Test was failing with: deadlock, also possibly with BDevStorage::findMutation: page_num not found (when autocommit is on)
    Resolution: added PyAPI lock before calling Fixture::onAutoCommit
    """
    def rand_string(str_len):
        return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
    
    db0.set_cache_size(100 << 10)
    buf = db0.list()
    py_buf = []
    for _ in range(20):
        for _ in range(100):
            str = rand_string(32)
            buf.append(MemoTestClass(str))
            py_buf.append(str)        
    
    index = 0
    for index in range(len(buf)):
        assert buf[index].value == py_buf[index]        
        index += 1


@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", 
                         [{"slab_size": 2 << 20, "autocommit": True}], indirect=True)
def test_low_cache_bad_address_issue1(db0_slab_size):
    """
    Test was failing with: CRDT_Allocator internal error: blank not found after ~6k operations
    Update: Fails faster depending on slab size but works fine with 1MB slab size    
    Update: disabling commits solves the issue (probably due to lack of deferred free-s)
    Resolution: 
    """
    def rand_string(max_len):
        str_len = random.randint(1, max_len)
        return ''.join(random.choice(string.ascii_letters) for i in range(str_len))

    root = MemoTestSingleton([])
    db0.set_cache_size(100 << 10)
    buf = root.value
    for _ in range(10000):
        buf.append([])
    
    # append to random lists
    count = 0    
    for _ in range(50000):
        str = rand_string(256)
        buf[random.randint(0, len(buf) - 1)].append(MemoTestClass(str))
        count += 1
        # commit every 1000 appends
        if count % 500 == 0:
            db0.commit()
            print(f"Transactions so far: {count}")
    