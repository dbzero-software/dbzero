import dbzero_ce as db0
from .conftest import MemoTestClass


def test_list_clear_produce_mutation_log_entry_issue(db0_fixture):
    """
    Issue: FIXME: https://github.com/wskozlowski/dbzero_ce/issues/403
    """
    with db0.locked() as lock:
        obj = MemoTestClass([1, 2, 3])
    assert len(obj.value) == 3
    assert len(lock.get_mutation_log()) == 1
    db0.commit()

    with db0.locked() as lock:
        obj.value.clear()
    assert len(obj.value) == 0
    assert len(lock.get_mutation_log()) == 1
        