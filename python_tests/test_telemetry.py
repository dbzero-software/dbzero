import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass
    

def rand_string(str_len):
    import random
    import string    
    return ''.join(random.choice(string.ascii_letters) for i in range(str_len))

    
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 4 * 1024 * 1024}], indirect=True)
def test_collecting_slab_metrics(db0_slab_size):
    buf = []
    str_size = [18, 1273, 133, 912, 993, 9213]
    for str_len in str_size:
        buf.append(MemoTestClass(rand_string(str_len)))
    
    slab_metrics = db0.get_slab_metrics()
    # 1 slab is user space, the other 2 are LSP & TYPE reserved slabs
    assert len(slab_metrics) == 3


@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 4 * 1024 * 1024}], indirect=True)
def test_remaining_capacity_metric(db0_slab_size):
    buf = []    
    trc1 = sum(slab["remaining_capacity"] for slab in db0.get_slab_metrics().values())
    str_size = [18, 1273, 133, 912, 993, 9213]
    for str_len in str_size:
        buf.append(MemoTestClass(rand_string(str_len)))
        
    trc2 = sum(slab["remaining_capacity"] for slab in db0.get_slab_metrics().values())
    assert  sum(str_size) / (trc1 - trc2) > 0.8
