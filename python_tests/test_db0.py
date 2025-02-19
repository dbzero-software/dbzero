import dbzero_ce as db0
from .memo_test_types import MemoTestClass,TriColor

def test_db0_init_close(db0_fixture):
    pass


def test_db0_can_open_long_prefix_name(db0_fixture):
    prefix_name = "/org/project/dev/some-long-prefix-name"
    db0.open(prefix_name)
    db0.drop(prefix_name)

def test_db0_is_memo(db0_fixture):
    assert db0.is_memo(MemoTestClass(1)) == True
    assert db0.is_memo(TriColor.RED) == False
    assert db0.is_memo(1) == False
    assert db0.is_memo("asd") == False
    assert db0.is_memo([1, 2, 3]) == False
    assert db0.is_memo({"a": 1, "b": 2}) == False
    assert db0.is_memo(db0.list([1,2,3])) == False