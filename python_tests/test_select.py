import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


def test_select_new(db0_fixture, memo_tags):
    db0.commit()
    state_1 = db0.get_state_num(finalized = True)
    obj_x = MemoTestClass(9999)
    db0.tags(obj_x).add("tag1")
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)
    # any python object can be used as a context
    context = lambda: None
    assert len(db0.select_new(db0.find(MemoTestClass), context, state_1)) == 11
    assert len(db0.select_new(db0.find(MemoTestClass), context, state_2)) == 1
    

def test_select_deleted(db0_fixture, memo_tags):
    db0.commit()
    state_1 = db0.get_state_num(finalized = True)    
    # un-tag single object
    obj_x = next(iter(db0.find(MemoTestClass, "tag1")))
    db0.tags(obj_x).remove("tag1")
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)    
    context = lambda: None
    assert len(db0.select_deleted(db0.find(MemoTestClass, "tag1"), context, state_1)) == 0
    assert len(db0.select_deleted(db0.find(MemoTestClass, "tag1"), context, state_2)) == 1


def test_select_modified_does_not_include_created(db0_fixture, memo_tags):
    db0.commit()
    state_1 = db0.get_state_num(finalized = True)
    obj_x = MemoTestClass(9999)
    db0.tags(obj_x).add("tag1")
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)
    context = lambda: None
    assert len(db0.select_modified(db0.find(MemoTestClass), context, state_1)) == 0
    # NOTE: there may be some number of false positives which we're unable to determine
    assert len(db0.select_modified(db0.find(MemoTestClass), context, state_2)) <= 10
    # but for sure, obj_x should not be in the result set
    for _obj in db0.select_modified(db0.find(MemoTestClass), context, state_2):
        assert _obj != obj_x


def test_select_modified_with_custom_filter(db0_fixture, memo_tags):
    def _compare(obj_1, obj_2):
        return obj_1.value == obj_2.value
    
    db0.commit()
    state_1 = db0.get_state_num(finalized = True)
    # modify 1 object in a separate transaction
    obj_1 = next(iter(db0.find(MemoTestClass, "tag1")))
    obj_1.value = 99999
    db0.commit()
    state_2 = db0.get_state_num(finalized = True)
    context = lambda: None
    query = db0.select_modified(db0.find(MemoTestClass), context, state_2, compare_with = _compare)
    for obj in query:
        print(obj)
    # FIXME: log
    # assert len(query) == 1
    # assert next(iter(query)).value == 99999
    