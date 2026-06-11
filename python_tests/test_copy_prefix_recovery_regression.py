# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import os
import multiprocessing
import time

import dbzero as db0

from .conftest import DB0_DIR, worker_path
from .memo_test_types import MemoTestClass, MemoTestSingleton


def _copy_prefix_live_writer(prefix, obj_count, commit_count, sleep_seconds):
    db0.init(DB0_DIR)
    db0.open(prefix, "rw")
    root = MemoTestSingleton([])
    for _ in range(commit_count):
        for _ in range(obj_count):
            root.value.append(MemoTestClass("b" * 1024))
        db0.commit()
        time.sleep(sleep_seconds)
    db0.close()


def test_copy_prefix_recovered_file_reopens_read_only(db0_fixture):
    copy_file_name = worker_path("./test-copy-recovery.db0")
    if os.path.exists(copy_file_name):
        os.remove(copy_file_name)

    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")

    root = MemoTestSingleton([])
    for _ in range(50):
        root.value.append(MemoTestClass("a" * 1024))
    db0.commit()

    db0.copy_prefix(copy_file_name)
    db0.close()

    os.remove(px_path)
    os.rename(copy_file_name, px_path)

    db0.init(DB0_DIR, prefix=px_name, read_write=False)
    root = db0.fetch(MemoTestSingleton)
    assert [item.value for item in root.value] == ["a" * 1024] * 50


def test_copy_closed_prefix_by_name_recovered_file_reopens_read_only(db0_fixture):
    copy_file_name = worker_path("./test-copy-closed-prefix.db0")
    if os.path.exists(copy_file_name):
        os.remove(copy_file_name)

    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")

    root = MemoTestSingleton([])
    for _ in range(5):
        root.value.append(MemoTestClass("a" * 1024))
    db0.commit()
    db0.close()

    db0.init(DB0_DIR)
    db0.copy_prefix(copy_file_name, prefix=px_name)
    db0.close()

    os.remove(px_path)
    os.rename(copy_file_name, px_path)

    db0.init(DB0_DIR, prefix=px_name, read_write=False)
    root = db0.fetch(MemoTestSingleton)
    assert [item.value for item in root.value] == ["a" * 1024] * 5


def test_copy_prefix_while_writer_active_then_final_copy_recovers(db0_fixture):
    live_copy_file_name = worker_path("./test-copy-live-prefix.db0")
    final_copy_file_name = worker_path("./test-copy-live-prefix-final.db0")
    for file_name in (live_copy_file_name, final_copy_file_name):
        if os.path.exists(file_name):
            os.remove(file_name)

    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")
    db0.close()

    obj_count = 500
    commit_count = 50
    writer = multiprocessing.Process(
        target=_copy_prefix_live_writer, args=(px_name, obj_count, commit_count, 0.01))
    writer.start()

    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    while writer.is_alive():
        try:
            if db0.exists(MemoTestSingleton) and len(db0.fetch(MemoTestSingleton).value) > obj_count:
                break
        except Exception:
            pass
        time.sleep(0.02)

    assert writer.is_alive()
    db0.copy_prefix(live_copy_file_name, prefix=px_name)
    writer.join()

    db0.copy_prefix(final_copy_file_name, prefix=px_name)
    db0.close()

    os.remove(px_path)
    os.rename(final_copy_file_name, px_path)

    db0.init(DB0_DIR, prefix=px_name, read_write=False)
    root = db0.fetch(MemoTestSingleton)
    assert len(root.value) == obj_count * commit_count
    assert [item.value for item in root.value] == ["b" * 1024] * (obj_count * commit_count)


def test_copy_prefix_repeated_live_copies_do_not_observe_unreadable_descriptor_diffs(db0_fixture):
    px_name = db0.get_current_prefix().name
    px_path = os.path.join(DB0_DIR, px_name + ".db0")
    db0.close()

    obj_count = 500
    commit_count = 120
    writer = multiprocessing.Process(
        target=_copy_prefix_live_writer, args=(px_name, obj_count, commit_count, 0.0))
    writer.start()

    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    while writer.is_alive():
        try:
            if db0.exists(MemoTestSingleton) and len(db0.fetch(MemoTestSingleton).value) > obj_count:
                break
        except Exception:
            pass
        time.sleep(0.01)

    assert writer.is_alive()
    copy_count = 0
    copy_file_names = []
    while writer.is_alive() and copy_count < 12:
        copy_file_name = worker_path(f"./test-copy-live-prefix-repeat-{copy_count}.db0")
        if os.path.exists(copy_file_name):
            os.remove(copy_file_name)
        db0.copy_prefix(copy_file_name, prefix=px_name)
        copy_file_names.append(copy_file_name)
        copy_count += 1

    writer.join()
    db0.close()
    assert copy_count > 1

    last_len = 0
    for copy_file_name in copy_file_names:
        os.remove(px_path)
        os.rename(copy_file_name, px_path)
        db0.init(DB0_DIR, prefix=px_name, read_write=False)
        root = db0.fetch(MemoTestSingleton)
        assert len(root.value) >= last_len
        assert all(item.value == "b" * 1024 for item in root.value)
        last_len = len(root.value)
        db0.close()
