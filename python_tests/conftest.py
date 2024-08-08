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


@pytest.fixture
def db0_slab_size(request):
    """
    DB0 scope with a very short autocommit interval
    """    
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", slab_size=request.param["slab_size"])
    yield db0
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


@pytest.fixture
def db0_autocommit_fixture(request):
    """
    DB0 scope with a very short autocommit interval
    """    
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR, autocommit_interval=request.param)
    db0.open("my-test-prefix")
    yield db0
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


@pytest.fixture()
def db0_no_autocommit():
    """
    DB0 scope with a very short autocommit interval
    """
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR)
    db0.open("my-test-prefix", autocommit=False)
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
def memo_excl_tags():
    """
    Exclusive tags (i.e. tags that are not shared between objects)
    """
    root = MemoTestSingleton([])
    str_tags = ["tag1", "tag2", "tag3", "tag4"]
    for i in range(10):
        object = MemoTestClass(i)
        root.value.append(object)        
        db0.tags(object).add(str_tags[i % 4])


@pytest.fixture()
def memo_enum_tags():
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    root = MemoTestSingleton([])
    colors = [Colors.RED, Colors.GREEN, Colors.BLUE]
    for i in range(10):
        object = MemoTestClass(i)
        root.value.append(object)
        db0.tags(object).add(colors[i % 3])
    return { "Colors": Colors }
