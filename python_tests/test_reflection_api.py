import pytest
import dbzero_ce as db0
import multiprocessing
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton


def test_get_prefixes(db0_fixture):
    assert len(list(db0.get_prefixes())) == 1    
    db0.open("my-new_prefix")
    assert len(list(db0.get_prefixes())) == 2
    
    
def test_get_memo_classes_from_default_prefix(db0_fixture):
    _ = MemoTestClass(123)
    assert len(list(db0.get_memo_classes())) > 0


def test_get_memo_classes_from_separate_process(db0_fixture):
    prefix_name = db0.get_current_prefix()
    _ = MemoTestClass(123)
    db0.commit()
    db0.close()
    
    def subprocess_get_memo_classes(result_queue):
        db0.init(DB0_DIR)
        db0.open(prefix_name)
        result_queue.put(list(db0.get_memo_classes()))
    
    # run from a subprocess
    result_queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=subprocess_get_memo_classes, args = (result_queue,))
    p.start()
    p.join()
    
    # validate the result
    memo_classes = result_queue.get()    
    assert len(memo_classes) > 0


def test_get_memo_classes_by_prefix_name(db0_fixture):
    prefix_name = db0.get_current_prefix()
    _ = MemoTestClass(123)
    assert len(list(db0.get_memo_classes(prefix_name))) > 0


def test_get_memo_classes_by_prefix_uuid(db0_fixture):
    _ = MemoTestClass(123)
    count = sum([len(list(db0.get_memo_classes(prefix_uuid=uuid))) for _, uuid in db0.get_prefixes()])
    assert count > 0


def test_get_memo_classes_by_prefix_name_and_uuid(db0_fixture):
    _ = MemoTestClass(123)
    count = sum([len(list(db0.get_memo_classes(prefix_name=name, prefix_uuid=uuid))) for name, uuid in db0.get_prefixes()])
    assert count > 0


def test_get_memo_classes_raises_when_mismatched_name_and_uuid(db0_fixture):
    _ = MemoTestClass(123)
    with pytest.raises(Exception):
        _ = [list(db0.get_memo_classes(prefix_name=name, prefix_uuid=123)) for name, _ in db0.get_prefixes()]
    
        
def test_get_memo_classes_returns_singletons(db0_fixture):
    root = MemoTestSingleton(123)
    _ = MemoTestClass(123)
    singletons = [obj for obj in db0.get_memo_classes() if obj.is_singleton]
    assert len(singletons) == 1
    # try accessing the singleton by UUID
    obj = singletons[0].get_instance()
    assert obj == root
    
   
def test_memo_class_get_attributes(db0_fixture):
    _ = MemoTestClass(123)
    memo_info = [obj for obj in db0.get_memo_classes() if not obj.is_singleton][0]
    assert len(list(memo_info.get_attributes())) > 0


def test_discover_tagged_objects(db0_fixture):
    obj = MemoTestClass(123)
    db0.tags(obj).add("tag1", "tag2")
    # using reflection API, identify memo classes
    memo_type = [obj for obj in db0.get_memo_classes()][0].get_type()
    # find all objects of this type
    assert len(list(db0.find(memo_type))) == 1
    