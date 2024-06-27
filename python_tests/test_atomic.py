import time
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


def test_new_object_inside_atomic_operation(db0_fixture):
    # this is to create a new class in dbzero
    MemoTestClass(123)    
    with db0.atomic() as atomic:
        object_2 = MemoTestClass(951)
        assert object_2.value == 951
        atomic.cancel()
    

def test_new_type_reverted_from_atomic_operation(db0_fixture):
    with db0.atomic() as atomic:
        # since MemoTestClass is used for the 1st time its type will be created
        object_1 = MemoTestClass(951)        
        atomic.cancel()
    # MemoTestClass type created here again (after atomic cancel)
    object_2 = MemoTestClass(123)
    assert object_2.value == 123
    
    
def test_query_after_atomic_cancel(db0_fixture):
    # this is to create a new class in dbzero
    object_1 = MemoTestClass(123)    
    with db0.atomic() as atomic:
        object_2 = MemoTestClass(951)
        atomic.cancel()
    assert object_1.value == 123


def test_read_after_atomic_create(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic():
        object_2 = MemoTestClass(951)
    assert object_1.value == 123
    assert object_2.value == 951


def test_read_after_atomic_update(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic():
        object_1.value = 951
    assert object_1.value == 951


def test_reading_after_atomic_cancel(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic() as atomic:
        object_1.value = 951
        atomic.cancel()
    assert object_1.value == 123
    
    
def test_assign_tags_inside_atomic_operation(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic() as atomic:
        db0.tags(object_1).add("tag1")
        assert len(list(db0.find("tag1"))) == 1        
    assert len(list(db0.find("tag1"))) == 1    
    
    
def test_assign_and_revert_tags_inside_atomic_operation(db0_fixture):
    object_1 = MemoTestClass(123)
    with db0.atomic() as atomic:
        db0.tags(object_1).add("tag1")
        assert len(list(db0.find("tag1"))) == 1        
        atomic.cancel()
    assert len(list(db0.find("tag1"))) == 0
    
    
def test_atomic_list_update(db0_fixture):
    object_1 = MemoTestClass([0])
    with db0.atomic() as atomic:
        object_1.value.append(1)
        object_1.value.append(2)
        object_1.value.append(3)

    assert object_1.value == [0, 1, 2, 3]
    
    
def test_atomic_revert_list_update(db0_fixture):
    object_1 = MemoTestClass([1,2])
    with db0.atomic() as atomic:
        object_1.value.append(3)
        object_1.value.append(4)
        object_1.value.append(5)
        atomic.cancel()
    
    assert object_1.value == [1, 2]
    