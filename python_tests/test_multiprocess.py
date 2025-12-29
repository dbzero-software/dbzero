# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from datetime import date, datetime
import pytest
import subprocess
import multiprocessing
import dbzero as db0
from python_tests.memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR

def test_hash_py_string(db0_fixture):
    assert db0.hash("abc") == db0.hash("abc")


def test_hash_enum(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])    
    assert db0.hash(Colors.RED) == db0.hash(Colors.RED)


def test_hash_py_tuple(db0_fixture):
    t1 = (1, "string", 999)
    t2 = (1, "string", 999)    
    assert db0.hash(t1) == db0.hash(t2)


def test_hash_py_tuple_with_enum(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    t1 = (1, Colors.RED, 999)
    t2 = (1, Colors.RED, 999)    
    assert db0.hash(t1) == db0.hash(t2)


def test_hash_with_db0_tuple(db0_fixture):
    t1 = db0.tuple([1, "string", 999])
    t2 = db0.tuple([1, "string", 999])    
    assert db0.hash(t1) == db0.hash(t2)


def test_hash_with_db0_tuple_with_enum(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    t1 = db0.tuple([1, Colors.RED, 999])
    t2 = db0.tuple([1, Colors.RED, 999])    
    assert db0.hash(t1) == db0.hash(t2)


def test_hash_with_db0_date(db0_fixture):
    d1 = date(2021, 12, 12)
    d2 = date(2021, 12, 12)    
    assert db0.hash(d1) == db0.hash(d2)


def test_hash_with_db0_datetime(db0_fixture):
    d1 = datetime(2021, 12, 12, 5, 5, 5)
    d2 = datetime(2021, 12, 12, 5, 5, 5)
    assert db0.hash(d1) == db0.hash(d2)


def get_test_without_remove(script, setup_script=""):
    return f"""
import os
import dbzero as db0
import shutil
import gc
DB0_DIR = os.path.join(os.getcwd(), "db0-test-data-subprocess/")
if not os.path.exists(DB0_DIR):
# create empty directory
    os.mkdir(DB0_DIR)
db0.init(DB0_DIR)
db0.open("my-test-prefix")
{setup_script}
print({script})
gc.collect()
db0.commit()
db0.close()
"""

def get_cleanup_script():
    return """
import os
import dbzero as db0
import shutil
import gc
DB0_DIR = os.path.join(os.getcwd(), "db0-test-data-subprocess/")
if os.path.exists(DB0_DIR):
    shutil.rmtree(DB0_DIR)
"""

def get_test_for_subprocess(value_to_hash, setup_script=""):
    return f"""
import os
import dbzero as db0
import shutil
import gc

DB0_DIR = os.path.join(os.getcwd(), "db0-test-data-subprocess/")
if os.path.exists(DB0_DIR):
    shutil.rmtree(DB0_DIR)
# create empty directory

os.mkdir(DB0_DIR)
db0.init(DB0_DIR)
db0.open("my-test-prefix")
{setup_script}
print({value_to_hash})
gc.collect()
db0.close()
if os.path.exists(DB0_DIR):
    shutil.rmtree(DB0_DIR)
"""

def get_hash(prefix_name, value, queue):
    db0.init(DB0_DIR)
    db0.open(prefix_name, "rw")
    result = db0.hash(value)
    queue.put(result)
    db0.close()
    return result


def get_enum_hash(prefix_name, enum_value, queue):
    db0.init(DB0_DIR)
    db0.open(prefix_name, "rw")
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    value = Colors[enum_value]
    result = db0.hash(value)
    queue.put(result)
    db0.close()
    return result


def run_hash_in_subprocess(prefix_name, value_to_hash, function=get_hash):
    queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=function, 
                            args=(prefix_name, value_to_hash, queue))
    p.start()
    p.join()
    if p.exitcode != 0:
        raise Exception(f"Subprocess failed with exit code {p.exitcode}")
    return queue.get()


def run_subprocess_script(script):
    result = subprocess.run(["python", "-c", script], capture_output=True)
    if result.returncode != 0:
        error_msg = f"Subprocess failed with return code {result.returncode}"
        if result.stderr:
            error_msg += f"\nStderr: {result.stderr.decode('latin-1')}"
        if result.stdout:
            error_msg += f"\nStdout: {result.stdout.decode('latin-1')}"
        raise Exception(error_msg)

    return result.stdout



def test_hash_strings_subprocess(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()
    sr1 = run_hash_in_subprocess(prefix_name, 'abc')
    sr2 = run_hash_in_subprocess(prefix_name, 'abc')
    assert sr1 == sr2


def test_hash_enum_subprocess(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()
    Colors = db0.enum('Colors', ['RED', 'GREEN', 'BLUE'])
    sr1 = run_hash_in_subprocess(prefix_name, str(Colors.RED), function=get_enum_hash)
    sr2 = run_hash_in_subprocess(prefix_name, str(Colors.RED), function=get_enum_hash)
    assert sr1 == sr2


def test_hash_tuple_subprocess(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()
    t1 = (1, 'string', 999)
    sr1 = run_hash_in_subprocess(prefix_name, t1)
    sr2 = run_hash_in_subprocess(prefix_name, t1)
    assert sr1 == sr2


def test_hash_bytes(db0_fixture):
    assert db0.hash(b"abc") == db0.hash(b"abc")


def test_hash_bytes_subprocess(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()
    sr1 = run_hash_in_subprocess(prefix_name, b'abc')
    sr2 = run_hash_in_subprocess(prefix_name, b'abc')
    assert sr1 == sr2

def check_dict_contains_key(prefix_name, key_uuid, dict_uuid, queue):
    db0.init(DB0_DIR)
    db0.open(prefix_name, "rw")
    key = db0.fetch(key_uuid)
    dictionary = db0.fetch(dict_uuid).value
    value = key in dictionary
    queue.put(value)
    db0.close()
    return value

def test_dict_comparison_when_executed_from_subprocess(db0_fixture):
    key = MemoTestClass("key")
    prefix_name = db0.get_current_prefix().name
    dictionary = db0.dict({key: "value"})
    key_uuid = db0.uuid(key)
    singleton = MemoTestSingleton(dictionary, key)
    uuid = db0.uuid(singleton)
    db0.commit()
    db0.close()
    
    queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=check_dict_contains_key, 
                            args=(prefix_name, key_uuid, uuid, queue))
    p.start()
    p.join()
    if p.exitcode != 0:
        raise Exception(f"Subprocess failed with exit code {p.exitcode}")
    assert queue.get()
    


def test_hash_date_subprocess(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()
    t1 = date(2021, 12, 12)
    sr1 = run_hash_in_subprocess(prefix_name, t1)
    sr2 = run_hash_in_subprocess(prefix_name, t1)
    assert sr1 == sr2

def test_hash_time_subprocess(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()
    from datetime import time
    t1 = time(12, 12, 12)
    sr1 = run_hash_in_subprocess(prefix_name, t1)
    sr2 = run_hash_in_subprocess(prefix_name, t1)
    assert sr1 == sr2

def test_hash_time_with_tz_subprocess(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()
    from datetime import timezone
    t1 = datetime(12, 12, 12, 5, 5, 5, tzinfo=timezone.utc).time()
    sr1 = run_hash_in_subprocess(prefix_name, t1)
    sr2 = run_hash_in_subprocess(prefix_name, t1)
    assert sr1 == sr2


def test_hash_datetime_subprocess(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()
    t1 = datetime(2021, 12, 12, 5, 5, 5)
    sr1 = run_hash_in_subprocess(prefix_name, t1)
    sr2 = run_hash_in_subprocess(prefix_name, t1)
    assert sr1 == sr2


def test_hash_datetime_with_tz_subprocess(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    db0.commit()
    db0.close()
    from datetime import timezone
    t1 = datetime(2021, 12, 12, 5, 5, 5, tzinfo=timezone.utc)
    sr1 = run_hash_in_subprocess(prefix_name, t1)
    sr2 = run_hash_in_subprocess(prefix_name, t1)
    assert sr1 == sr2