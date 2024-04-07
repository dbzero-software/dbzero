import pytest
import dbzero_ce as db0
import random
import string


@db0.memo
class IntMock:
    def __init__(self, bytes):
        self.bytes = bytes


# def test_sigsegv(db0_fixture):
#     int_1 = 1
#     test_int= IntMock(int_1)
#     # This assert segfaults due to pytest trying to print description of test_int.bytes
#     assert test_int.bytes == 2


""" def test_write_free_read_random_bytes(db0_fixture):
    # FIXME: should be executed in dbg mode only
    alloc_cnt = 10
    data = {}
    addr_list = []
    for _ in range(5):
        for _ in range(alloc_cnt):
            # random size (up to 512 bytes)
            k = random.randint(1, 3666)
            str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=k))        
            addr = db0.dbg_write_bytes(str)
            data[addr] = str
            addr_list.append(addr)
        
        # free 20% of addresses
        for _ in range(int(alloc_cnt * 0.33)):
            n = random.randint(0, len(addr_list) - 1)
            db0.dbg_free_bytes(addr_list[n])
            del data[addr_list[n]]
            del addr_list[n]
        
        #for addr, str in data.items():
        #    assert db0.dbg_read_bytes(addr) == str
 """