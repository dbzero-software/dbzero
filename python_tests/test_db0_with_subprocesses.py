from datetime import date, datetime
import pytest
import subprocess
import dbzero_ce as db0
from python_tests.memo_test_types import MemoTestClass, MemoTestSingleton


def test_hash_py_string():  
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
import dbzero_ce as db0
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
import dbzero_ce as db0
import shutil
import gc
DB0_DIR = os.path.join(os.getcwd(), "db0-test-data-subprocess/")
if os.path.exists(DB0_DIR):
    shutil.rmtree(DB0_DIR)
"""

def get_test_for_subprocess(value_to_hash, setup_script=""):
    return f"""
import os
import dbzero_ce as db0
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

def run_subprocess_script(script):
    result = subprocess.run(["python3", "-c", script], capture_output=True)
    if result.returncode != 0:
        print(result.stderr)
        raise Exception("Error in subprocess")

    return result.stdout

def test_hash_strings_subprocess():
    subprocess_script = get_test_for_subprocess("db0.hash('abc')")
    sr1 = run_subprocess_script(subprocess_script)
    
    sr2 = run_subprocess_script(subprocess_script)
    assert sr1 == sr2

def test_hash_enum_subprocess():
    setup= """
Colors = db0.enum('Colors', ['RED', 'GREEN', 'BLUE'])
"""
    subprocess_script = get_test_for_subprocess('db0.hash(Colors.RED)', setup)
    sr1 = run_subprocess_script(subprocess_script)
    
    sr2 = run_subprocess_script(subprocess_script)
    assert sr1 == sr2

def test_hash_tuple_subprocess():
    setup= """
t1 = (1, 'string', 999)
"""
    subprocess_script = get_test_for_subprocess('db0.hash(t1)', setup)
    sr1 = run_subprocess_script(subprocess_script)
    
    sr2 = run_subprocess_script(subprocess_script)
    assert sr1 == sr2

def test_hash_bytes():
    assert db0.hash(b"abc") == db0.hash(b"abc")

def test_hash_bytes_subprocess():
    subprocess_script = get_test_for_subprocess("db0.hash(b'abc')")
    sr1 = run_subprocess_script(subprocess_script)
    sr2 = run_subprocess_script(subprocess_script)
    assert sr1 == sr2

def test_dict_comparison_when_executed_from_subprocess():
    cleanup = get_cleanup_script()
    run_subprocess_script(cleanup)
    setup = '''
from python_tests.memo_test_types import MemoTestClass, MemoTestSingleton
key = MemoTestClass("key")
dictionary = db0.dict({key: "value"})
key_uuid = db0.uuid(key)
uuid = db0.uuid(dictionary)
singleton = MemoTestSingleton(dictionary, key)
db0.commit()
'''
    script = "f'{key_uuid},{uuid}'"
    subprocess_script = get_test_without_remove(script, setup)
    sr1 = run_subprocess_script(subprocess_script)
    key_uuid, uuid = sr1[:-1].decode("UTF-8").split(",")
    
    setup = f"""
from python_tests.memo_test_types import MemoTestClass, MemoTestSingleton
key = db0.fetch('{key_uuid}')
dict = db0.fetch('{uuid}')
value = key in dict
"""
    script = "value"
    subprocess_script = get_test_without_remove(script, setup)
    sr1 = run_subprocess_script(subprocess_script)
    assert sr1 == b"True\n"
    run_subprocess_script(cleanup)


def test_hash_date_subprocess():
    setup= """
from datetime import date
t1 = date(2021, 12, 12)
"""
    subprocess_script = get_test_for_subprocess('db0.hash(t1)', setup)
    sr1 = run_subprocess_script(subprocess_script)
    
    sr2 = run_subprocess_script(subprocess_script)
    assert sr1 == sr2

def test_hash_time_subprocess():
    setup= """
from datetime import time
from datetime import timezone
t1 = time(12, 12, 12)
"""
    subprocess_script = get_test_for_subprocess('db0.hash(t1)', setup)
    sr1 = run_subprocess_script(subprocess_script)
    
    sr2 = run_subprocess_script(subprocess_script)
    assert sr1 == sr2

def test_hash_time_with_tz_subprocess():
    setup= """
from datetime import datetime
from datetime import timezone
t1 = datetime(12, 12, 12, 5, 5, 5, tzinfo=timezone.utc).time()
"""
    subprocess_script = get_test_for_subprocess('db0.hash(t1)', setup)
    sr1 = run_subprocess_script(subprocess_script)
    
    sr2 = run_subprocess_script(subprocess_script)
    assert sr1 == sr2


def test_hash_datetime_subprocess():
    setup= """
from datetime import datetime
t1 = datetime(2021, 12, 12, 5, 5, 5)
"""
    subprocess_script = get_test_for_subprocess('db0.hash(t1)', setup)
    sr1 = run_subprocess_script(subprocess_script)
    
    sr2 = run_subprocess_script(subprocess_script)
    assert sr1 == sr2


def test_hash_datetime_with_tz_subprocess():
    setup= """
from datetime import datetime
from datetime import timezone
t1 = datetime(2021, 12, 12, 5, 5, 5, tzinfo=timezone.utc)
"""
    subprocess_script = get_test_for_subprocess('db0.hash(t1)', setup)
    sr1 = run_subprocess_script(subprocess_script)
    
    sr2 = run_subprocess_script(subprocess_script)
    assert sr1 == sr2