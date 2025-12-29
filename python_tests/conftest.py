# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

"""Conftest for test package"""
# pylint: disable=redefined-outer-name
import os
import pytest
import gc
import dbzero as db0
import shutil
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoDataPxClass, \
        MemoDataPxSingleton, DATA_PX


TEST_FILES_DIR_ROOT = os.path.join(os.getcwd(), "python_tests", "files")
DB0_DIR = os.path.join(os.getcwd(), "db0-test-data")


def __extract_param(request, key, default):
    if hasattr(request, "param") and request.param and key in request.param:
        return request.param[key]
    return default

@pytest.fixture()
def db0_fixture(request):
    if 'D' in db0.build_flags():
        db0.reset_test_params() # reset to defaults
        db0.enable_storage_validation(__extract_param(request, "storage_validation", False))        
        
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)    
    os.mkdir(DB0_DIR)
    db0.init(
        DB0_DIR
    )
    db0.open(
        "my-test-prefix",
        # use custom page_io_step_size if specified in request.param
        page_io_step_size=__extract_param(request, "page_io_step_size", None),
        autocommit=__extract_param(request, "autocommit", True)
    )
    yield db0
    gc.collect()
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


@pytest.fixture()
def db0_no_default_fixture():  
    if 'D' in db0.build_flags():
        db0.reset_test_params() # reset to defaults

    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR)
    yield db0
    gc.collect()
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


@pytest.fixture
def db0_slab_size(request):
    if 'D' in db0.build_flags():
        db0.reset_test_params() # reset to defaults
    
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    db0.init(
        DB0_DIR,
        autocommit=request.param.get("autocommit", True),
        autocommit_interval=request.param.get("autocommit_interval", 250)
    )
    db0.open("my-test-prefix", slab_size=request.param["slab_size"])
    yield db0     
    gc.collect()
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


@pytest.fixture
def db0_autocommit_fixture(request):
    """
    dbzero scope with a very short autocommit interval
    """    
    if 'D' in db0.build_flags():
        db0.reset_test_params() # reset to defaults

    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR, autocommit=True, autocommit_interval=request.param)
    db0.open("my-test-prefix")
    yield db0    
    gc.collect()
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


@pytest.fixture()
def db0_no_autocommit():
    if 'D' in db0.build_flags():
        db0.reset_test_params() # reset to defaults

    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    # disable autocommit on all prefixes
    db0.init(DB0_DIR, autocommit=False)
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


@pytest.fixture()
def memo_scoped_enum_tags():
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"], prefix=DATA_PX)
    root = MemoDataPxSingleton([])
    colors = [Colors.RED, Colors.GREEN, Colors.BLUE]
    for i in range(10):
        object = MemoDataPxClass(i)
        root.value.append(object)
        db0.tags(object).add(colors[i % 3])
    return { "Colors": Colors }


@pytest.fixture()
def db0_metaio_fixture():
    """
    dbzero scope for testing metaio (very small step size)
    """ 
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR, autocommit=False)
    db0.open("my-test-prefix", meta_io_step_size=16)
    yield db0
    gc.collect()
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


@pytest.fixture()
def db0_large_lang_cache_no_autocommit():
    """
    dbzero scope for testing large language cache (no autocommit)
    """
    if 'D' in db0.build_flags():
        db0.reset_test_params() # reset to defaults

    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    # create empty directory
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR, autocommit=False, lang_cache_size=16 << 20, cache_size= 16 << 30)
    db0.open("my-test-prefix")
    yield db0    
    db0.close()    
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)