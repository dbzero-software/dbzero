import pytest
from datetime import datetime
import time
import dbzero_ce as db0
import random
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


def test_autocommit_can_be_disabled_for_prefix(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close(prefix_name)
    db0.open(prefix_name, autocommit=False)
    state_1 = db0.get_state_num()
    object_1 = MemoTestClass(951)
    time.sleep(0.3)
    state_2 = db0.get_state_num()
    # no autocommit, state not changed
    assert state_1 == state_2
    
    
@pytest.mark.parametrize("db0_autocommit_fixture", [10], indirect=True)
def test_dict_items_not_in_while_autocommit(db0_autocommit_fixture):
    dict_1 = db0.dict()
    for _ in range(100000):
        assert 5 not in dict_1
        

def test_autocommit_disabled_by_fixture(db0_no_autocommit):
    prefix = db0.get_current_prefix()
    db0.commit()
    db0.close(prefix.name)
    db0.open(prefix.name, autocommit=False)
    state_1 = db0.get_state_num()
    object_1 = MemoTestClass(951)
    time.sleep(0.3)
    state_2 = db0.get_state_num()
    # no autocommit, state not changed
    assert state_1 == state_2
    

@db0.memo()
class Task:
    def __init__(self,deadline):
        self.deadline = deadline
        self.runs = []


@pytest.mark.stress_test
@pytest.mark.parametrize("db0_autocommit_fixture", [1], indirect=True)
def test_autocommit_with_commit_crash_issue(db0_autocommit_fixture):
    count = 0
    for _ in range(5000):
        task = Task(datetime.now())
        db0.commit()
        count += 1
        if count % 1000 == 0:
            print(f"Processed {count} tasks")
            
            
@pytest.mark.parametrize("db0_autocommit_fixture", [500], indirect=True)
def test_dict_items_in_segfault_issue_1(db0_autocommit_fixture):
    """
    This test was failing with segfault when autocommit enabled.
    Must be repeated at least 15-20 times to reproduce the issue.
    """
    dict_1 = db0.dict()
    item_count = 100
    for i in range(item_count):
        dict_1[i] = i
    for i in range(100000):
        random_int = random.randint(0, 300)
        if random_int < item_count:
            assert random_int in dict_1
        else:
            assert random_int not in dict_1


@pytest.mark.parametrize("db0_autocommit_fixture", [500], indirect=True)
def test_list_items_append(db0_autocommit_fixture):
    """
    This test was failing with segfault when autocommit enabled.
    Must be repeated at least 15-20 times to reproduce the issue.
    """
    list_1 = db0.list()
    item_count = 100
    for i in range(item_count):
        list_1.append(i)
    for i in range(item_count):
        list_1[i] = 2*i
    for i in range(100000):
        random_int = random.randint(0, 300)
        if random_int < item_count * 2 and random_int % 2 == 0:
            assert random_int in list_1
        else:
            assert random_int not in list_1