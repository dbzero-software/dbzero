import pytest
import dbzero_ce as db0


def test_low_level_write_bytes(db0_fixture):
    # Context: this simple test-case was crashing the process (due to memory corruption)    
    objects = []
    for i in range(100):    
        objects.append({ "test": 123 })
