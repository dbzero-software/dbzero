import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


def test_memo_singleton_is_created_once(db0_fixture):
    object_1 = MemoTestSingleton(999, "text")
    object_2 = MemoTestSingleton()
    assert repr(db0.uuid(object_1)) == repr(db0.uuid(object_2))


def test_memo_singleton_can_be_accessed_with_fetch(db0_fixture):
    object_1 = MemoTestSingleton(999, "text")  
    object_2 = db0.fetch(MemoTestSingleton)
    assert repr(db0.uuid(object_1)) == repr(db0.uuid(object_2))


def test_memo_singleton_can_be_deleted(db0_fixture):
    object_1 = MemoTestSingleton(999, "text")
    db0.delete(object_1)
    with pytest.raises(Exception):
        object_2 = MemoTestSingleton()


def test_singleton_can_be_fetched_by_id(db0_fixture):
    prefix = db0.get_current_prefix()
    object_x = MemoTestClass("x")
    object_1 = MemoTestSingleton(999, object_x)
    id1 = db0.uuid(object_1)
    id2 = db0.uuid(object_x)
    db0.commit()
    db0.close()

    db0.init(DB0_DIR)
    db0.open(prefix.name, "r")
    assert db0.is_singleton(db0.fetch(id1))
    assert not db0.is_singleton(db0.fetch(id2))
