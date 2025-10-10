import dbzero_ce as db0
from .conftest import MemoTestSingleton
from .conftest import DB0_DIR


def test_memo_class_persistence_issue1(db0_fixture):
    px_name = db0.get_current_prefix().name
    obj = MemoTestSingleton(1)
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    assert MemoTestSingleton().value == 1
    