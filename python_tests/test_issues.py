import pytest
import dbzero_ce as db0

@db0.memo
class IntMock:
    def __init__(self, bytes):
        self.bytes = bytes


# def test_sigsegv(db0_fixture):
#     int_1 = 1
#     test_int= IntMock(int_1)
#     # This assert segfaults due to pytest trying to print description of test_int.bytes
#     assert test_int.bytes == 2