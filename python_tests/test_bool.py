
import dbzero_ce as db0
from datetime import datetime

@db0.memo
class MemoScopedClass:
    def __init__(self, value, prefix=None):
        db0.set_prefix(self, prefix)        
        self.value = value   

def test_assing_bool_to_memo(db0_fixture):
    m = MemoScopedClass(True)
    assert m.value == True
    m = MemoScopedClass(False)
    assert m.value == False
