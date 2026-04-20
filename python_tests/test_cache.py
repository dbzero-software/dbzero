# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import multiprocessing
import dbzero as db0
from random import randint
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton
    

def get_string(str_len):
    return 'a' * str_len    
    

def rand_array(max_bytes):
    import random
    actual_len = random.randint(1, max_bytes)
    return [1] * (int(actual_len / 8) + 1)

    
def test_cache_size_can_be_updated_at_runtime(db0_fixture):
    cache_0 = db0.get_cache_stats()    
    # create object instances to populate cache
    buf = []
    for _ in range(1000):
        buf.append(MemoTestClass(get_string(1024)))
    cache_1 = db0.get_cache_stats()    
    diff_1 = cache_1["size"] - cache_0["size"]
    # reduce cache size so that only 1/2 of objects can fit
    db0.set_cache_size(512 * 1024)
    cache_2 = db0.get_cache_stats()    
    # make sure cache size / capacity was adjusted with at least 95% accuracy
    assert abs(1.0 - (512 * 1024) / cache_2["size"]) < 0.05
    assert abs(1.0 - cache_2["capacity"] / cache_2["size"]) < 0.05
    
    
def test_base_lock_usage_does_not_exceed_limits(db0_fixture):
    # run this test only in debug mode
    if 'D' in db0.build_flags():
        append_count = 200
        cache_size = 1024 * 1024
        db0.set_cache_size(cache_size)
        usage_1 = db0.get_base_lock_usage()[0]
        for _ in range(append_count):
            _ = MemoTestClass(rand_array(16384))
        db0.clear_cache()
        import gc
        gc.collect()        
        usage_2 = db0.get_base_lock_usage()[0]
        print("usage diff = ", usage_2 - usage_1)
        assert usage_2 - usage_1 < cache_size * 1.5


def test_lang_cache_can_reach_capacity(db0_fixture):
    buf = db0.list()
    # python instances are added to lang cache until it reaches capacity
    initial_capacity = db0.get_lang_cache_stats()["capacity"]    
    for _ in range(initial_capacity * 2):        
        buf.append(MemoTestClass(123))
    # here we call commit to flush from internal buffers (e.g. TagIndex)
    db0.commit()            
    # capacity not changed
    assert db0.get_lang_cache_stats()["capacity"] == initial_capacity
    # capacity might be exceeded due to indeterministic gc collection by Python
    assert db0.get_lang_cache_stats()["size"] < initial_capacity * 2


# --- Writer process used by the stale-instance repro below.
# The writer cycles through: create obj, commit, (await reader), delete obj,
# create a replacement obj (likely reusing the same physical slot), commit.
def _writer_create_delete_create(prefix_name, req_queue, resp_queue):
    db0.init(DB0_DIR)
    db0.open(prefix_name, "rw")
    # singleton holds a strong reference so we control exactly when the
    # writer-side instance is dropped
    root = MemoTestSingleton(None)
    while True:
        cmd = req_queue.get()
        if cmd is None:
            break
        action = cmd[0]
        if action == "create":
            value = cmd[1]
            obj = MemoTestClass(value)
            root.value = obj
            uuid = db0.uuid(obj)
            del obj
            db0.commit()
            resp_queue.put(uuid)
        elif action == "delete":
            # drop the only strong reference and commit so the object
            # and its slab slot are freed on disk before the next create
            root.value = None
            db0.commit()
            resp_queue.put("ok")
    db0.close()


def test_lang_cache_returns_stale_instance_after_slot_reuse(db0_fixture):
    """
    Reproduces a lang-cache coherence bug on read-only clients.

    Scenario: a writer creates obj1, the reader fetches it into its
    LangCache. The writer then deletes obj1 and creates obj2, which is
    likely allocated at obj1's recycled physical address. After refresh,
    the reader fetches obj2 by its new UUID; the client-side LangCache
    is keyed by (fixture_id, address) - no instance_id - so it returns
    the stale wrapper that was bound to obj1's (now recycled) slot.

    The test records UUIDs only - it intentionally does not keep Python
    references to fetched wrappers. Holding a stale wrapper around
    exposes a separate "stale instance access" class of bugs which is
    out of scope here; dropping the wrapper after each fetch isolates
    the LangCache coherence issue. The cache entry itself survives the
    wrapper being dropped (it holds its own reference via
    ObjectSharedExtPtr) until something explicitly clears it.
    """
    prefix_name = db0.get_current_prefix().name
    # make sure the prefix exists on disk before the reader opens it read-only
    db0.commit()
    db0.close()

    req_queue = multiprocessing.Queue()
    resp_queue = multiprocessing.Queue()
    writer = multiprocessing.Process(
        target=_writer_create_delete_create,
        args=(prefix_name, req_queue, resp_queue),
    )
    writer.start()

    # Record (uuid, expected_value) pairs seen across all iterations so we
    # can also validate post-loop that fetching any historical uuid either
    # succeeds with the right value or fails cleanly - never silently
    # returns a stale/wrong instance.
    seen = []
    try:
        db0.init(DB0_DIR)
        db0.open(prefix_name, "r")
        iterations = 50
        for i in range(iterations):
            first_value = i
            second_value = 100000 + i

            # writer: create obj1 (instance 0)
            req_queue.put(("create", first_value))
            uuid1 = resp_queue.get(timeout=30)

            db0.refresh()
            obj1 = db0.fetch(uuid1)
            assert obj1.value == first_value, \
                f"iter {i}: unexpected obj1.value={obj1.value}"
            assert db0.uuid(obj1) == uuid1, \
                f"iter {i}: uuid1 roundtrip failed"
            # intentionally drop the wrapper immediately - we only retain
            # the uuid, not the Python instance
            del obj1

            # writer: delete obj1 and commit
            req_queue.put(("delete",))
            assert resp_queue.get(timeout=30) == "ok"

            # writer: create obj2 (instance 1) - likely reuses obj1's slot
            req_queue.put(("create", second_value))
            uuid2 = resp_queue.get(timeout=30)
            assert uuid1 != uuid2

            db0.refresh()
            obj2 = db0.fetch(uuid2)
            # If the LangCache wrongly served the stale entry for obj1's
            # address, db0.uuid(obj2) will not match the uuid we just
            # resolved through, and obj2.value may report obj1's value.
            assert db0.uuid(obj2) == uuid2, (
                f"iter {i}: uuid mismatch - lang cache served stale wrapper "
                f"(expected {uuid2}, got {db0.uuid(obj2)})"
            )
            assert obj2.value == second_value, (
                f"iter {i}: lang cache returned stale instance "
                f"(expected {second_value}, got {obj2.value}); "
                f"uuid1={uuid1} uuid2={uuid2}"
            )
            seen.append((uuid2, second_value))
            del obj2
    finally:
        req_queue.put(None)
        writer.join(timeout=30)
        if writer.is_alive():
            writer.terminate()
            writer.join()