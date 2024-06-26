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
    

def test_new_type_inside_atomic_operation(db0_fixture):
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
    