import time
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


def test_db0_starts_autocommit_by_default(db0_fixture):
    object_1 = MemoTestClass(951)
    state_1 = db0.get_state_num()
    # auto-commit should happen no later than within 250ms    
    time.sleep(0.3)
    state_2 = db0.get_state_num()
    # state changed due to autocommit
    assert state_2 > state_1
    # update object
    object_1.value = 952
    time.sleep(0.3)
    state_3 = db0.get_state_num()
    assert state_3 > state_2
    time.sleep(0.3)
    state_4 = db0.get_state_num()
    # state not changed (no modifications performed)
    assert state_4 == state_3


def test_autocommit_is_not_performed_during_mutations(db0_fixture):
    object_1 = MemoTestClass(951)
    state_1 = db0.get_state_num()
    # perform mutations for 300ms
    start = time.time()
    while time.time() - start < 0.3:
        time.sleep(0.01)
        object_1.value += 1    
    state_2 = db0.get_state_num()
    # state should not change during mutations
    assert state_2 == state_1
    time.sleep(0.3)
    state_2 = db0.get_state_num()
    assert state_2 > state_1
