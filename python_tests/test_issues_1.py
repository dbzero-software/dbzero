# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
import random
import string
import time
import multiprocessing
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


@db0.memo
class IntMock:
    def __init__(self, bytes):
        self.bytes = bytes


def test_write_free_random_bytes(db0_fixture):
    # run this test only in debug mode
    if 'D' in db0.build_flags():        
        alloc_cnt = 10
        data = {}
        addr_list = []
        for _ in range(5):
            for _ in range(alloc_cnt):
                # random size (large enough to cause boundary locks)
                k = random.randint(1, 3666)
                str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=k))        
                addr = db0.dbg_write_bytes(str)
                data[addr] = str
                addr_list.append(addr)
            
            # free 1/3 of addresses
            for _ in range(int(alloc_cnt * 0.33)):
                n = random.randint(0, len(addr_list) - 1)
                db0.dbg_free_bytes(addr_list[n])
                del data[addr_list[n]]
                del addr_list[n]
    

def test_write_read_random_bytes(db0_fixture):
    # run this test only in debug mode
    if 'D' in db0.build_flags():
        data = {}
        addr_list = []
        for _ in range(2):
            k = random.randint(3000, 3500)
            str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=k))        
            addr = db0.dbg_write_bytes(str)
            data[addr] = str
            addr_list.append(addr)
                
        for addr, str in data.items():
            assert db0.dbg_read_bytes(addr) == str


def test_write_free_read_random_bytes(db0_fixture):
    # run this test only in debug mode
    if 'D' in db0.build_flags():
        alloc_cnt = 100
        for _ in range(25):
            data = {}
            addr_list = []
            for _ in range(alloc_cnt):
                k = random.randint(20, 500)
                str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=k))        
                addr = db0.dbg_write_bytes(str)
                data[addr] = str
                addr_list.append(addr)
            
            # free 1/3 of addresses
            for _ in range(int(alloc_cnt * 0.33)):
                n = random.randint(0, len(addr_list) - 1)
                db0.dbg_free_bytes(addr_list[n])
                del data[addr_list[n]]
                del addr_list[n]
            
            for addr, str in data.items():
                assert db0.dbg_read_bytes(addr) == str


def test_write_free_read_random_bytes_in_multiple_transactions(db0_fixture):
    # run this test only in debug mode
    if 'D' in db0.build_flags():
        num_transactions = 5
        alloc_cnt = 25
        # ks = [random.randint(20, 500) for _ in range(alloc_cnt * num_transactions)]
        # ns = []
        # for _ in range(num_transactions):
        #     ns.extend([random.randint(0, alloc_cnt - 1 - i) for i in range(int(alloc_cnt * 0.33))])
        
        ks = [325, 316, 105, 432, 211, 346, 41, 441, 154, 254, 431, 498, 263, 70, 428, 151, 48, 44, 121, 207, 487, 331, 110, 296, 456, 500, 450, 251, 
              453, 380, 423, 374, 385, 162, 369, 442, 263, 353, 498, 69, 109, 298, 30, 244, 450, 64, 489, 105, 336, 275, 178, 23, 218, 462, 70, 144, 
              100, 308, 239, 494, 406, 60, 396, 257, 207, 223, 35, 244, 254, 377, 419, 151, 201, 281, 187, 189, 121, 305, 245, 287, 20, 490, 312, 461, 
              306, 301, 290, 473, 104, 218, 380, 30, 349, 159, 477, 452, 440, 345, 429, 336, 84, 318, 191, 58, 42, 239, 385, 23, 119, 59, 105, 383, 
              120, 393, 339, 115, 174, 380, 500, 323, 102, 487, 300, 474, 332]
        
        ns = [0, 13, 11, 11, 18, 12, 0, 2, 6, 3, 16, 8, 17, 4, 13, 2, 5, 14, 16, 17, 1, 6, 17, 0, 2, 4, 7, 13, 6, 9, 12, 0, 8, 4, 12, 5, 3, 6, 17, 14]
        
        k_values = iter(ks)
        n_values = iter(ns)
        for _ in range(num_transactions):
            data = {}
            addr_list = []
            for _ in range(alloc_cnt):
                k = next(k_values)
                str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=k))        
                addr = db0.dbg_write_bytes(str)
                data[addr] = str
                addr_list.append(addr)
            
            # free 1/3 of addresses
            for _ in range(int(alloc_cnt * 0.33)):
                n = next(n_values)                
                db0.dbg_free_bytes(addr_list[n])
                del data[addr_list[n]]
                del addr_list[n]
            
            db0.commit()            
            for addr, str in data.items():                
                assert db0.dbg_read_bytes(addr) == str


def test_print_type(db0_fixture):
    # this test got sigsev on invalid PyObjectType initialization from PyHeapTypeObject
    int_1 = db0.list([1,2,3])
    test_int= IntMock(int_1)
    print(test_int)
    print(type(test_int))
    
    
def test_db0_commit_close_issue_1(db0_fixture):
    """
    The problem was due to collect using incorrect object type
    """
    object_x = MemoTestClass(123123)
    prefix = db0.get_current_prefix()
    
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(prefix.name, "rw")
    
    
def make_small_update(px_name, expected_values):
    time.sleep(0.25)
    db0.init(DB0_DIR)
    db0.open(px_name, "rw")
    note = MemoTestClass(expected_values[0])
    db0.tags(note).add("tag")
    db0.commit()                
    time.sleep(0.25)
    if 'D' in db0.build_flags():            
        db0.dbg_start_logs()
    note.value = expected_values[1]
    db0.close()

    
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1 << 20}], indirect=True)
def test_refresh_issue1(db0_slab_size):
    """
    Issue: process blocked on refresh attempt
    Reason: missing SparsePair.commit() call when finishing a transaction
    """
    px_name = db0.get_current_prefix().name
    expected_values = ["first string", "second string"]
        
    rand_ints = [350, 480, 343, 475, 871, 493, 550, 723, 342, 236, 110, 585, 633, 54, 797, 478, 850, 716, 1021, 
                 136, 248, 879, 151, 249, 15, 717, 773, 625, 738, 731, 955, 280, 208, 730, 754, 982, 281, 221, 
                 549, 501, 282, 307, 551, 472, 509, 761, 78, 735, 744, 450, 388, 645, 577, 706, 417, 78, 849, 
                 873, 904, 534, 945, 985, 431, 725, 826, 49, 64, 766, 32, 460, 971, 766, 390, 990, 899, 835, 
                 16, 570, 190, 573, 54, 642, 840, 817, 924, 793, 634, 889, 835, 250, 676, 1006, 819, 322, 
                 373, 278, 895, 767, 380, 442]                 
    
    index = 0
    root = MemoTestSingleton([])
    for _ in range(10000):
        str_len = rand_ints[index]
        root.value.append(''.join("A" for i in range(str_len)))
        index += 1
        if index == len(rand_ints):
            index = 0
    db0.close()
    time.sleep(1)
    p = multiprocessing.Process(target=make_small_update, 
                                args=(px_name, expected_values))
    p.start()
    
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    
    for i in range(2):
        state_num = db0.get_state_num(px_name)    
        # refresh until 2 transactions are detected
        max_repeat = 30
        if i == 1 and 'D' in db0.build_flags():
            db0.dbg_start_logs()
        
        while db0.get_state_num(px_name) == state_num:
            assert max_repeat > 0
            db0.refresh()
            time.sleep(0.1)
            max_repeat -= 1
        assert next(iter(db0.find(MemoTestClass))).value == expected_values[i]
        max_repeat -= 1
    
    p.join()
    
    