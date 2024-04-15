import pytest
import dbzero_ce as db0
import random
import string


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
        alloc_cnt = 25
        for _ in range(5):
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
            
            db0.commit()
            for addr, str in data.items():
                assert db0.dbg_read_bytes(addr) == str


def test_print_type(db0_fixture):
    # this test got sigsev on invalid PyObjectType initialization from PyHeapTypeObject
    int_1 = db0.list([1,2,3])
    test_int= IntMock(int_1)
    print(test_int)
    print(type(test_int))
