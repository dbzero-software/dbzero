import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


def test_persisting_single_transaction_data(db0_fixture):
    prefix_name = db0.get_current_prefix().name
    root = MemoTestSingleton([])
    # add objects to root
    for i in range(100):
        root.value.append(MemoTestClass(i))
    # drop from python (but no from db0)
    del root
    # now, close db0 and then reopen (transaction performed on close)
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    root = MemoTestSingleton()
    # check if the objects are still there
    for i in range(100):
        assert root.value[i].value == i


def test_persisting_data_in_multiple_transactions(db0_fixture):
    prefix = db0.get_current_prefix()
    root = MemoTestSingleton([])
    # add objects to root (in 10 transactions)
    for i in range(10):
        for j in range(10):
            root.value.append(MemoTestClass(i * 10 + j))
        db0.commit()
    # drop from python (but no from db0)
    del root
    # now, close db0 and then reopen (transaction performed on close)
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix.name)
    root = MemoTestSingleton()
    # check if the objects are still there
    num = 0
    for obj in root.value:
        assert obj.value == num
        num += 1


def test_persisting_data_in_multiple_independent_transactions(db0_fixture):
    prefix = db0.get_current_prefix()
    root = MemoTestSingleton([])
    # add objects to root (in 10 transactions)
    # close and reopen db0 after each transaction
    for i in range(10):
        for j in range(10):
            root.value.append(MemoTestClass(i * 10 + j))
        db0.commit()
        db0.close()
        db0.init(DB0_DIR)
        db0.open(prefix.name)
        # need to reopen since python object is no longer accessible after close
        root = MemoTestSingleton()
    
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix.name)
    root = MemoTestSingleton()    
    num = 0
    for obj in root.value:
        assert obj.value == num
        num += 1
