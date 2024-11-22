import pytest
import subprocess
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton


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