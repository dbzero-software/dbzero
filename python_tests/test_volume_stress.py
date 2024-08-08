import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass

    
@pytest.mark.stress_test
@pytest.mark.parametrize("db0_slab_size", [{"slab_size": 1024 * 1024 * 1024}], indirect=True)
def test_create_large_objects_bitset_issue(db0_slab_size):
    """
    This test was failing on BitsetAllocator:invalid address, 
    the effect if aggravated by the large slab size
    """
    def rand_string(max_len):
        import random
        import string
        actual_len = random.randint(1, max_len)
        return ''.join(random.choice(string.ascii_letters) for i in range(actual_len))
    
    append_count = 1000
    buf = []
    total_bytes = 0
    count = 0
    report_bytes = 1024 * 1024
    for _ in range(append_count):
        buf.append(MemoTestClass(rand_string(16 * 1024 + 8192)))
        total_bytes += len(buf[-1].value)
        count += 1
        if total_bytes > report_bytes:
            print(f"Total bytes: {total_bytes}")
            report_bytes += 1024 * 1024
        if count % 1000 == 0:
            print(f"Objects created: {count}")
