# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import multiprocessing
import queue
import time
import dbzero as db0
import os
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton
    

def append_to_prefix(prefix, obj_count = 50, commit_count = 50, long_run = False):
    db0.init(DB0_DIR)
    db0.open(prefix, "rw")
    # create new or open an existing root object
    root = MemoTestSingleton([])
    if (len(root.value) > 0):
        print(f"Writer process: opened existing prefix with {len(root.value)} objects")
    for i in range(commit_count):
        for _ in range(obj_count):
            root.value.append(MemoTestClass("b" * 1024))  # 1 KB string
        db0.commit()
        if long_run:
            print(f"Writer process: committed {(i + 1) * obj_count} objects", flush=True)
        else:
            time.sleep(0.1)
    
    if long_run:
        print(db0.get_storage_stats(), flush=True)    
    db0.close()    
    

def validate_current_prefix(expected_len = None, expected_min_len = None):
    # refresh to assure we have latest data
    db0.refresh()
    # NOTE: reader process needs to use snapshots for concurrency safety
    with db0.snapshot() as snap:
        root = snap.fetch(MemoTestSingleton)
        print("--- begin iterate / validation", flush=True)
        assert not expected_min_len or len(root.value) >= expected_min_len
        assert not expected_len or len(root.value) == expected_len
        for item in root.value:
            assert item.value == "b" * 1024
        print(f"--- end iterate len = {len(root.value)}", flush=True)
        return len(root.value)


def rand_string(str_len):
    import random
    import string    
    return ''.join(random.choice(string.ascii_letters) for i in range(str_len))


def create_process_refresh_query_while_adding(px_name, num_iterations,
                                              num_objects, str_len):
    db0.init(DB0_DIR)
    db0.open(px_name, "rw")
    for _ in range(num_iterations):          
        for index in range(num_objects):
            obj = MemoTestClass(rand_string(str_len))
            db0.tags(obj).add("tag1")
            if index % 3 == 0:
                db0.tags(obj).add("tag2")            
        db0.commit()
    db0.close()


def _get_sparse_pair_manager_refresh_stress_config():
    # Increase DB0_SPM_REFRESH_STRESS_SECONDS or set DB0_SPM_REFRESH_STRESS_MAX_COMMITS=0
    # for open-ended long-duration runs. Large fast-writer settings are expected
    # to exercise SparsePairManager refresh catch-up aggressively.
    return {
        "duration_seconds": float(os.environ.get("DB0_SPM_REFRESH_STRESS_SECONDS", "10")),
        "batch_size": int(os.environ.get("DB0_SPM_REFRESH_STRESS_BATCH_SIZE", "256")),
        "payload_size": int(os.environ.get("DB0_SPM_REFRESH_STRESS_PAYLOAD_SIZE", "2048")),
        "max_commits": int(os.environ.get("DB0_SPM_REFRESH_STRESS_MAX_COMMITS", "200")),
        "reader_sleep_seconds": float(os.environ.get("DB0_SPM_REFRESH_STRESS_READER_SLEEP", "0.01")),
        "catch_up_seconds": float(os.environ.get("DB0_SPM_REFRESH_STRESS_CATCH_UP_SECONDS", "60")),
    }


def _sparse_pair_manager_refresh_writer(px_name, config, result_queue):
    try:
        db0.init(DB0_DIR)
        db0.open(px_name, "rw")
        root = MemoTestSingleton([])
        start_time = time.monotonic()
        commit_count = 0
        total_count = 0

        while True:
            if config["max_commits"] and commit_count >= config["max_commits"]:
                break
            if time.monotonic() - start_time >= config["duration_seconds"]:
                break

            payload = f"{commit_count:08d}-" + ("x" * config["payload_size"])
            for _ in range(config["batch_size"]):
                root.value.append(MemoTestClass(payload))
            db0.commit()

            commit_count += 1
            total_count += config["batch_size"]
            if commit_count % 10 == 0:
                result_queue.put(("progress", total_count))

        result_queue.put(("done", total_count))
        db0.close()
    except BaseException as exc:
        result_queue.put(("error", repr(exc)))
        try:
            db0.close()
        except BaseException:
            pass


@pytest.mark.stress_test
@pytest.mark.parametrize("stress_config", [_get_sparse_pair_manager_refresh_stress_config()])
def test_sparse_pair_manager_sparse_indexes_refresh_under_long_running_updates(db0_fixture, stress_config):
    root = MemoTestSingleton([])
    px_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()

    result_queue = multiprocessing.Queue()
    writer = multiprocessing.Process(
        target=_sparse_pair_manager_refresh_writer,
        args=(px_name, stress_config, result_queue),
    )
    writer.start()

    final_count = None
    last_seen_count = 0
    refresh_count = 0
    last_state_num = 0
    last_refresh_result = None
    start_time = time.monotonic()
    writer_timeout_seconds = max(30.0, stress_config["duration_seconds"] * 4)
    catch_up_start_time = None

    try:
        db0.init(DB0_DIR)
        db0.open(px_name, "r")
        while True:
            try:
                while True:
                    event, value = result_queue.get_nowait()
                    if event == "error":
                        raise AssertionError(f"writer failed: {value}")
                    if event == "done":
                        final_count = value
                        catch_up_start_time = time.monotonic()
            except queue.Empty:
                pass

            last_refresh_result = db0.refresh()
            refresh_count += 1
            last_state_num = db0.get_state_num(px_name)

            with db0.snapshot() as snap:
                root = snap.fetch(MemoTestSingleton)
                current_count = len(root.value)
                assert current_count >= last_seen_count
                if current_count:
                    first = root.value[0].value
                    last = root.value[current_count - 1].value
                    assert isinstance(first, str) and first.endswith("x" * stress_config["payload_size"])
                    assert isinstance(last, str) and last.endswith("x" * stress_config["payload_size"])
                last_seen_count = current_count

            if final_count is not None and last_seen_count >= final_count:
                break
            if final_count is None and time.monotonic() - start_time > writer_timeout_seconds:
                raise AssertionError(
                    f"writer did not finish: seen={last_seen_count}, refresh_count={refresh_count}"
                )
            if (catch_up_start_time is not None
                    and time.monotonic() - catch_up_start_time > stress_config["catch_up_seconds"]):
                raise AssertionError(
                    f"reader did not catch up: seen={last_seen_count}, final={final_count}, "
                    f"refresh_count={refresh_count}, state_num={last_state_num}, "
                    f"last_refresh_result={last_refresh_result}"
                )
            time.sleep(stress_config["reader_sleep_seconds"])

        writer.join(timeout=5)
        assert writer.exitcode == 0
        assert final_count is not None
        assert last_seen_count == final_count
        assert refresh_count > 0
    finally:
        if writer.is_alive():
            writer.terminate()
        writer.join()
        db0.close()


@pytest.mark.stress_test
def test_refresh_query_while_adding_new_objects(db0_fixture):
    px_name = db0.get_current_prefix().name
    
    db0.commit()
    db0.close()
    
    num_iterations = 1
    num_objects = 1000
    str_len = 4096
    p = multiprocessing.Process(target=create_process_refresh_query_while_adding, 
                                args = (px_name, num_iterations, num_objects, str_len))
    p.start()
    
    try:
        db0.init(DB0_DIR)
        db0.open(px_name, "r")
        while True:
            db0.refresh()
            time.sleep(0.1)
            query_len = len(list(db0.find(MemoTestClass, "tag1")))        
            print(f"Query length: {query_len}")
            if query_len == num_iterations * num_objects:
                break
    finally:
        p.terminate()
        p.join()
        db0.close()

@pytest.mark.skip(reason="https://github.com/dbzero-software/dbzero/issues/662")
@pytest.mark.stress_test
def test_continuous_refresh_process(db0_fixture):
    px_name = db0.get_current_prefix().name
    db0.close()
    
    # in each 'epoch' we modify prefix while making copies
    # then drop the original prefix and restore if from the last copy
    epoch_count = 2
    total_len = 0
    for epoch in range(epoch_count):
        print(f"=== Epoch {epoch} ===")
        obj_count = 5000
        commit_count = 100
        # start the writer process for a long run
        p = multiprocessing.Process(target=append_to_prefix, args=(px_name, obj_count, commit_count, True))
        p.start()
        
        db0.init(DB0_DIR)
        db0.open(px_name, "r")
        last_len = 0
        while True:
            # NOTE: reader needs to use snapshots for concurrency safety
            with db0.snapshot() as snap:
                if not snap.exists(MemoTestSingleton):
                    time.sleep(0.1)
                    continue
                root = snap.fetch(MemoTestSingleton)
                if len(root.value) > 1:
                    last_len = len(root.value)
                    break
            time.sleep(0.1)
        
        # validate prefix while writer is actively modifying it
        while True:        
            if not p.is_alive():
                break
            print("--- Validate  prefix iteration", flush=True)            
            last_len = validate_current_prefix(expected_min_len = last_len)
            print(f"--- Prefix valid with {last_len} objects", flush=True)
            if not p.is_alive():
                break
            time.sleep(0.25)
        
        p.terminate()
        p.join()
        total_len += obj_count * commit_count
        
        print("Validating final prefix ...", flush=True)         
        validate_current_prefix(expected_len = total_len)        
        db0.close()
