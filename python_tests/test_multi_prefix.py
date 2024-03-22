import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton


def test_memo_objects_can_be_created_on_different_prefixes(db0_fixture):
    # create DBZero object over a default prefix
    object_1 = MemoTestClass(123)
    # open the 2nd prefix which becomes default
    db0.open("some-other-prefix")
    object_2 = MemoTestClass(456)
    assert db0.get_prefix(object_1) != db0.get_prefix(object_2)


def test_current_prefix_is_changed_by_open(db0_fixture):
    prefix_name = db0.get_current_prefix()
    other_prefix_name = "some-other-prefix"
    db0.open(other_prefix_name)
    assert db0.get_current_prefix() != prefix_name
    assert db0.get_current_prefix() == other_prefix_name


def test_current_prefix_is_restored_to_latest_after_close(db0_fixture):
    prefix_name = db0.get_current_prefix()
    other_prefix_name = "some-other-prefix"
    db0.open(other_prefix_name)
    assert db0.get_current_prefix() != prefix_name
    db0.close(other_prefix_name)
    # current prefix should be restored to the original
    assert db0.get_current_prefix() == prefix_name


def test_prefix_is_auto_opened_when_accessed(db0_fixture):
    other_prefix_name = "some-other-prefix"
    db0.open(other_prefix_name)    
    object_1 = MemoTestClass(123)
    # keep reference to this object from the singleton to prevent from deletion
    singleton = MemoTestSingleton(object_1)
    object_uuid = db0.uuid(object_1)
    # delete python instances only
    del object_1
    del singleton
    db0.commit()
    db0.close(other_prefix_name)
    
    # try accessing object from closed prefix (should be auto-opened by uuid)
    object_2 = db0.fetch(object_uuid)
    assert object_2.value == 123
    
    
def test_can_commit_specific_prefix(db0_fixture):
    other_prefix_name = "some-other-prefix"
    db0.open(other_prefix_name)
    object_1 = MemoTestClass(123)    
    db0.commit(other_prefix_name)
