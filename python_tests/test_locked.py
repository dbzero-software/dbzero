import time
import pytest
from .memo_test_types import MemoTestClass
import dbzero_ce as db0


def test_locked_section_can_be_entered(db0_fixture):
    with db0.locked():
        obj = MemoTestClass(951)
        obj_uuid = db0.uuid(obj)        
    # make sure object exists outside of locked section
    assert db0.fetch(obj_uuid) == obj
    

def test_locked_section_reporting_updated_current_prefix(db0_fixture):
    px_name = db0.get_current_prefix().name
    with db0.locked() as lock:
        obj = MemoTestClass(951)
    # collect the mutation log after exiting the locked section
    mutation_log = lock.get_mutation_log()
    assert px_name in [name for name, _ in mutation_log]
    
    
def test_locked_section_no_mutations(db0_fixture):
    obj = MemoTestClass(951)
    with db0.locked() as lock:
        # read/only operation
        obj_2 = db0.fetch(db0.uuid(obj))
    
    assert len(lock.get_mutation_log()) == 0
    
    
def test_locked_section_non_default_prefix_mutations(db0_fixture):
    obj_1 = MemoTestClass(951)
    px_name = db0.get_current_prefix()
    db0.open("some-new-prefix", "rw")
    obj_2 = MemoTestClass(952)
    obj_3 = MemoTestClass(953)
    # switch back to px_name as the default prefix
    db0.open(px_name.name)
    with db0.locked() as lock:        
        # update non-default prefix bound object
        obj_2.value = 123123
        obj_3.value = 91237
        
    mutation_log = lock.get_mutation_log()
    assert px_name not in [name for name, _ in mutation_log]
    assert "some-new-prefix" in [name for name, _ in mutation_log]    
    