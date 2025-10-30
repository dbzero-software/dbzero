from random import random
import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, TriColor, MemoAnyAttrs
from dataclasses import dataclass
import random
import string


def rand_string(max_len):    
    str_len = random.randint(1, max_len)
    return ''.join(random.choice(string.ascii_letters) for i in range(str_len))


@db0.memo(no_cache=True)
@dataclass
class MemoNoCacheClass:
    data: str
    
    def __init__(self):
        self.data = rand_string(12 << 10)
    
        
def test_create_memo_no_cache(db0_fixture):
    obj_1 = MemoNoCacheClass()
    assert obj_1.data is not None
