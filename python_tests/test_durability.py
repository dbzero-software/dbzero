import pytest
import dbzero_ce as db0
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


def test_opening_prefix_of_crashed_process(db0_no_default_fixture):
    def open_prefix_then_crash():
        db0.open("new-prefix-1")
        db0.tags(MemoTestClass(123)).add("tag1", "tag2")
        # end process with exception before commit / close
        raise Exception("Crash!")

    p = multiprocessing.Process(target=open_prefix_then_crash)
    p.start()
    p.join()
    
    # try opeining the crashed prefix for read
    db0.open("new-prefix-1", "r")
    assert len(list(db0.find("tag1"))) == 0
    

def test_modify_prefix_of_crashed_process(db0_no_default_fixture):
    def open_prefix_then_crash():
        db0.open("new-prefix-1")
        db0.tags(MemoTestClass(123)).add("tag1", "tag2")
        # end process with exception before commit / close
        raise Exception("Crash!")
    
    p = multiprocessing.Process(target=open_prefix_then_crash)
    p.start()
    p.join()
    
    # try opeining the crashed prefix for read/write and append some objects
    db0.open("new-prefix-1", "rw")
    db0.tags(MemoTestClass(123)).add("tag1", "tag2")    
    db0.commit()
    
    
def test_durability_of_random_objects_issue1(db0_no_default_fixture):
    """
    This test was failing with an exception when opening the prefix:
    BDevStorage::findMutation: page_num 0 not found, state: 10
    Resolution: the problem was related to DirtyCache assuming page_size as pow 2 while DRAM_Prefix page size is not    
    """
    def rand_string(max_len):
        import random
        import string
        actual_len = random.randint(1, max_len)
        return ''.join(random.choice(string.ascii_letters) for i in range(actual_len))
    
    append_count = 1000
    def create_objects():
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
    
    p = multiprocessing.Process(target=create_objects)
    p.start()
    p.join()
    
    # read the created objects
    db0.open("new-prefix-1", "r")
    root = MemoTestSingleton()
    assert len(root.value) == append_count


def test_dump_dram_io_map(db0_fixture):
    if 'D' in db0.build_flags():
        io_map = db0.get_dram_io_map()
        print(io_map)
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

# FIXME: failing test blocked
# def test_low_cache_transactions_issue1(db0_no_autocommit):
#     """
#     Test was failing with: element mismatch (when running with very small cache size)
#     Resolution: ???
#     """
#     def rand_string(str_len):
#         return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
    
#     db0.set_cache_size(64 << 10)
#     buf = db0.list()
#     py_buf = []
#     for _ in range(2):
#         for _ in range(34):
#             str = rand_string(64)            
#             buf.append(str)
#             py_buf.append(str)
#         db0.commit()
    
#     index = 0
#     for index in range(len(buf)):
#         # assert buf[index].value == py_buf[index]        
#         assert buf[index] == py_buf[index]
#         index += 1
    
    
# def test_low_cache_transactions_issue2(db0_no_autocommit):
#     """
#     Test was failing with: CRDT_Allocator.cpp, line 157: Invalid address: NNN
#     NOTE: due to random nature may need to run 5 - 7x times to reproduce
#     Resolution: ???
#     """
#     def rand_string(str_len):
#         return ''.join(random.choice(string.ascii_letters) for i in range(str_len))
    
#     db0.set_cache_size(100 << 10)
#     buf = db0.list()
#     py_buf = []
#     for _ in range(2):
#         for _ in range(25):
#             str = rand_string(64)
#             buf.append(MemoTestClass(str))
#             py_buf.append(str)
#         db0.commit()
#         # FIXME: log
#         print("^^^ commit ^^^")
    
#     index = 0
#     for index in range(len(buf)):
#         assert buf[index].value == py_buf[index]        
#         index += 1
