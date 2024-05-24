"""Conftest for test package"""
# pylint: disable=redefined-outer-name
import os
import pytest
import dbzero_ce as db0
import shutil
from .memo_test_types import MemoTestClass, MemoTestSingleton


TEST_FILES_DIR_ROOT = os.path.join(os.getcwd(), "python_tests", "files")
DB0_DIR = os.path.join(os.getcwd(), "db0-test-data")


@pytest.fixture()
def db0_fixture():
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR)    
    db0.open("my-test-prefix")
    yield db0
    db0.close()  
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


@pytest.fixture()
def memo_tags():
    root = MemoTestSingleton([])
    for i in range(10):
        object = MemoTestClass(i)
        root.value.append(object)
        db0.tags(object).add("tag1")
        if i % 2 == 0:
            db0.tags(object).add("tag2")
        if i % 3 == 0:
            db0.tags(object).add("tag3")
        if i % 4 == 0:
            db0.tags(object).add("tag4")


@pytest.fixture()
def enum_tags():
    Colors = db0.enum("Colors", ["red", "green", "blue", "black"])
    root = MemoTestSingleton([])
    for i in range(10):
        object = MemoTestClass(i)
        root.value.append(object)
        db0.tags(object).add("tag1")
        if i % 2 == 0:
            db0.tags(object).add(Colors.red)
        if i % 3 == 0:
            db0.tags(object).add(Colors.green)
        if i % 4 == 0:
            db0.tags(object).add(Colors.blue)
