"""Conftest for test package"""
# pylint: disable=redefined-outer-name
import os
import pytest
import dbzero_ce as db0
import shutil


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
    