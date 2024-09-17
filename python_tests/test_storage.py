import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestSingleton
from .conftest import DB0_DIR
    

def test_db0_state_can_be_persisted_and_then_retrieved_for_read(db0_fixture):
    # create singleton
    object_1 = MemoTestSingleton(999, "text")
    prefix_name = db0.get_prefix_of(object_1)
    # commit data and close db0
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    # open db0 as read-only
    db0.open(prefix_name, "r")    
    object_2 = MemoTestSingleton()
    assert object_2.value == 999
    assert object_2.value_2 == "text"


def test_db0_state_modify_without_close(db0_fixture):
    # create singleton
    object_1 = MemoTestSingleton(999, "text")
    prefix_name = db0.get_prefix_of(object_1)
    # commit data and close db0
    db0.commit()    
    
    object_1.value = 888
    db0.commit()
    assert object_1.value == 888


def test_dynamic_fields_are_persisted(db0_fixture):
    # create singleton
    object_1 = MemoTestSingleton(999, "text")
    object_1.kv_field = 91123
    prefix_name = db0.get_prefix_of(object_1)
    object_2 = db0.fetch(db0.uuid(object_1))
    # commit data and close db0
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")
    object_2 = MemoTestSingleton()
    assert object_2.kv_field == 91123
