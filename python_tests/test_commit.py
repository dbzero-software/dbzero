import dbzero_ce as db0
from .memo_test_types import MemoTestClass


def test_create_instances_in_multiple_transactions(db0_fixture):
    group_size = 10
    for x in range(10):
        for i in range(group_size):
            obj = MemoTestClass(i)
            db0.tags(obj).add("tag1")
        
        db0.commit()        
        assert len(list(db0.find("tag1"))) == group_size * (x + 1)
