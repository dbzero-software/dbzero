# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton
import multiprocessing


def test_recover_after_crash_during_commit(db0_fixture):
    # This test only works with the debug build
    if not "D" in db0.build_flags():
        return
    
    def rand_chars(length):
        import random
        import string
        return ''.join(random.choice(string.ascii_letters) for _ in range(length))

    _ = MemoTestSingleton([], [])
    px_name = db0.get_current_prefix().name
    expected_values = [rand_chars(512) for _ in range(1000)]    
    
    # start a child process that will change the singleton
    def generator_process(op_size, op_count, crash_after):
        db0.init(DB0_DIR, autocommit=False)
        db0.open(px_name, "rw")
        root = MemoTestSingleton()
        next_id = len(root.value)
        for i in range(op_count):
            for _ in range(op_size):
                root.value.append(MemoTestClass(next_id))                
                root.value_2.append(expected_values[next_id])
                next_id += 1
            if crash_after is not None and i == crash_after[0]:
                # activate the write poison to simulate a crash
                if crash_after[1]:
                    db0.set_test_params(dram_io_flush_poison = crash_after[1])
                else:
                    db0.set_test_params(write_poison = 3)
            db0.commit()
        # NOTE: db0.close is not called if the process crashes
        db0.close()
    
    # close db0 and open as read-only
    db0.commit()
    db0.close()
    
    # NOTE: the 2nd process will crash during the 3rd commit
    # the 3rd process will be killed during DRAM IO flush
    crash_after = [None, (3, None), (2, 2), None]
    op_size = 50
    op_count = 5
    for poison in crash_after:
        p = multiprocessing.Process(target=generator_process, args=(op_size, op_count, poison))
        p.start()
        p.join()
    
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    # NOTE: we expect the 2 cycles to be fully completed
    # and 2 to be partially completed
    expected_len = op_size * op_count * 2 + (5 * op_size)
    assert len(MemoTestSingleton().value) == expected_len
    assert len(MemoTestSingleton().value_2) == expected_len

    for i, obj in enumerate(MemoTestSingleton().value):
        assert obj.value == i
    
    for i, value in enumerate(MemoTestSingleton().value_2):
        assert value == expected_values[i]
    