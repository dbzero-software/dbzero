# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
from datetime import datetime
import time
import dbzero as db0
import random
from .memo_test_types import MemoTestClass
from .conftest import DB0_DIR

@pytest.mark.skip(reason="This test is time reliant and may fail on slow CI systems")
def test_db0_starts_autocommit_by_default(db0_fixture):
    object_1 = MemoTestClass(951)
    commit_interval = db0.get_config()['autocommit_interval']
    state_1 = db0.get_state_num()
    # wait as long as autocommit interval + 100ms margin
    time.sleep(commit_interval / 1000.0 + 0.1)
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


def test_autocommit_is_not_performed_during_atomic_mutations(db0_fixture):
    object_1 = MemoTestClass(951)
    state_1 = db0.get_state_num()
    # perform atomic mutations for 350ms
    with db0.atomic():
        start = time.time()        
        while time.time() - start < 0.35:
            time.sleep(0.01)
            object_1.value += 1    
        state_2 = db0.get_state_num()
    # state should not change during mutations
    assert state_2 == state_1


def test_autocommit_can_be_disabled_for_prefix(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close(prefix_name)
    db0.open(prefix_name, autocommit=False)
    state_1 = db0.get_state_num()
    object_1 = MemoTestClass(951)
    time.sleep(0.350)
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


def test_autocommit_config(db0_fixture):
    cfg = db0.get_config()
    assert cfg['autocommit'] == True
    default_interval = cfg['autocommit_interval']

    db0.close()
    db0.init(DB0_DIR, autocommit=False, autocommit_interval=1000)
    cfg = db0.get_config()
    assert cfg['autocommit'] == False
    assert cfg['autocommit_interval'] == 1000

    db0.close()
    db0.init(DB0_DIR, autocommit=False)
    cfg = db0.get_config()
    assert cfg['autocommit'] == False
    assert cfg['autocommit_interval'] == default_interval

    db0.close()
    with pytest.raises(Exception):
        cfg = db0.get_config()


@pytest.mark.skip(reason="This test is time reliant and may fail on slow CI systems")
@pytest.mark.parametrize("db0_autocommit_fixture", [100], indirect=True)
def test_autocommit_tagging(db0_autocommit_fixture):
    # ISSUE: https://github.com/wskozlowski/dbzero/issues/435
    obj = MemoTestClass(123)
    db0.commit()
    state1 = db0.get_state_num()

    db0.tags(obj).add("some_tag")
    time.sleep(0.3)
    state2 = db0.get_state_num()
    assert state2 > state1

    db0.tags(obj).remove("some_tag")
    time.sleep(0.3)
    state3 = db0.get_state_num()
    assert state3 > state2


@pytest.mark.skip(reason="This test is time reliant and may fail on slow CI systems")
@pytest.mark.parametrize("db0_autocommit_fixture", [100], indirect=True)
def test_autocommit_index_operations(db0_autocommit_fixture):
    idx = db0.index()
    obj = MemoTestClass(123)
    db0.commit()
    state1 = db0.get_state_num()

    idx.add(123, obj)
    time.sleep(0.3)
    state2 = db0.get_state_num()
    assert state2 > state1

    idx.remove(123, obj)
    time.sleep(0.3)
    state3 = db0.get_state_num()
    assert state3 > state2