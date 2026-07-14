# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestSingleton, MemoTestClass
from .conftest import DB0_DIR
import random
import string

    
def test_db0_state_can_be_persisted_and_then_retrieved_for_read(db0_fixture):
    # create singleton
    object_1 = MemoTestSingleton(999, "text")
    prefix_name = db0.get_prefix_of(object_1).name
    # commit data and close db0
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    # open db0 as read-only
    db0.open(prefix_name, "r")    
    object_2 = MemoTestSingleton()
    assert object_2.value == 999
    assert object_2.value_2 == "text"


def test_db0_state_modify_without_close(db0_fixture):
    # create singleton
    object_1 = MemoTestSingleton(999, "text")
    prefix_name = db0.get_prefix_of(object_1).name
    # commit data and close db0
    db0.commit()    
    
    object_1.value = 888
    db0.commit()
    assert object_1.value == 888


def test_dynamic_fields_are_persisted(db0_fixture):
    # create singleton
    object_1 = MemoTestSingleton(999, "text")
    object_1.kv_field = 91123
    prefix_name = db0.get_prefix_of(object_1).name
    object_2 = db0.fetch(db0.uuid(object_1))
    # commit data and close db0
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")
    object_2 = MemoTestSingleton()
    assert object_2.kv_field == 91123


@pytest.mark.stress_test
def test_storage_utilization_without_commits(db0_fixture):
    buf = db0.list()
    
    def rand_str():
        return ''.join(random.choice(string.ascii_letters) for i in range(1000))    
    
    total_bytes = 0
    for _ in range(10):
        for _ in range(1000):
            str = rand_str()
            total_bytes += len(str)
            buf.append(MemoTestClass(str))
        db0.commit()
    dp_size_total = db0.get_storage_stats()['dp_size_total']
    file_size = db0.get_storage_stats()['prefix_size']
    # make sure storage overhead is < 25%
    assert dp_size_total < 1.25 * total_bytes
    assert file_size < 1.25 * total_bytes


@pytest.mark.parametrize("db0_fixture", [{"storage_validation": True}], indirect=True)
def test_generate_data_with_storage_validation(db0_fixture):
    if 'D' in db0.build_flags():
        # very small cache size to trigger lots of IO operations
        db0.set_cache_size(128 << 10)
        buf = db0.list()
        
        def rand_str():
            return ''.join(random.choice(string.ascii_letters) for i in range(1000))    
        
        total_bytes = 0
        for _ in range(50):
            for _ in range(100):
                str = rand_str()
                total_bytes += len(str)
                buf.append(MemoTestClass(str))
            db0.commit()
