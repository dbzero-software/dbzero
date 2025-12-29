# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from itertools import islice
from datetime import datetime
from .memo_test_types import MemoTestClass, MemoTask
import random


def test_create_instances_in_multiple_transactions(db0_fixture):
    group_size = 10
    for x in range(10):
        for i in range(group_size):
            obj = MemoTestClass(i)
            db0.tags(obj).add("tag1")
        
        db0.commit()
        assert len(list(db0.find("tag1"))) == group_size * (x + 1)


def test_append_list_in_multiple_transactions(db0_fixture):
    # prepare instances first
    tasks = db0.list()
    db0.commit()
    for _ in range(10):
        for _ in range(5):
            tasks.append(MemoTask("etl", "processor1"))
        db0.commit()
    
    assert len(tasks) == 50


def test_create_types_in_multiple_transactions(db0_fixture):
    MemoTestClass(123)    
    db0.commit()
    # type MemoTask created in a new transaction
    MemoTask("etl", "processor1")


def test_append_list_member_in_multiple_transactions(db0_fixture):
    root = MemoTestClass(db0.list())
    db0.commit()
    for _ in range(10):
        for _ in range(5):            
            root.value.append(MemoTask("etl", "processor1"))
        db0.commit()
            
    assert len(root.value) == 50


def test_untag_instances_in_multiple_transactions(db0_fixture):
    # prepare instances first
    count = 10
    for _ in range(count):
        task = MemoTask("etl", "processor1")
        db0.tags(task).add("ready")
    
    db0.commit()
    repeats = 0
    while count > 0:
        tasks = list(islice(db0.find("ready"), 2))
        for task in tasks:
            task.runs.append(MemoTestClass(1))
            
        count -= len(tasks)
        repeats += 1
        db0.tags(*tasks).remove("ready")
        db0.tags(*tasks).add("running")
        db0.commit()
    
    assert repeats == 5
    
    
def test_commit_state_num_issue_1(db0_fixture, memo_tags):
    """
    Issue: the finalized state number was not reported correctly (showing old transaction number after commit)
    Resolution: on commit, the TagIndex::flush was called AFTER GC0::commit - which caused setting incRef / modification
    flag on objects which later resulted in missing dirty flags after actual mutations
    """    
    db0.commit()    
    state_1 = db0.get_state_num(finalized = True)
    state_pending = db0.get_state_num(finalized = False)
    assert state_pending > state_1
    obj_1 = next(iter(db0.find(MemoTestClass, "tag1")))
    obj_1.value = 99999    
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)
    assert state_2 == state_pending


def test_is_dirty_assert_issue(db0_fixture):
    """
    This test was failing in debug mode on !ResourceLock::isDirty() assert
    Resolution: 
    """    
    def get_string(actual_len):
        return ''.join(' ' for i in range(actual_len))
    
    with db0.atomic():
        _ = MemoTestClass(get_string(5053))
    