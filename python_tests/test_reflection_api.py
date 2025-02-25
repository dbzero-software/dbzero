import pytest
import dbzero_ce as db0
import multiprocessing
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoTestClassWithMethods
from datetime import datetime


def test_get_prefixes(db0_fixture):
    assert len(list(db0.get_prefixes())) == 1
    db0.open("my-new_prefix")
    assert len(list(db0.get_prefixes())) == 2
    
    
def test_get_prefixes_with_nested_dirs(db0_fixture):
    assert len(list(db0.get_prefixes())) == 1
    db0.open("dir_1/my-new_prefix")
    db0.open("dir_1/subdir/my-new_prefix")
    db0.open("dir_2/subdir1/subdir2/my-new_prefix")
    assert len(list(db0.get_prefixes())) == 4

    
def test_get_memo_classes_from_default_prefix(db0_fixture):
    _ = MemoTestClass(123)
    assert len(list(db0.get_memo_classes())) > 0


def test_get_memo_classes_from_separate_process(db0_fixture):
    prefix = db0.get_current_prefix()
    _ = MemoTestClass(123)
    db0.commit()
    db0.close()
    
    def subprocess_get_memo_classes(result_queue):
        db0.init(DB0_DIR)
        db0.open(prefix.name)
        result_queue.put(list(db0.get_memo_classes()))
    
    # run from a subprocess
    result_queue = multiprocessing.Queue()
    p = multiprocessing.Process(target=subprocess_get_memo_classes, args = (result_queue,))
    p.start()
    p.join()
    
    # validate the result
    memo_classes = result_queue.get()    
    assert len(memo_classes) > 0


def test_get_raw_memo_classes_by_prefix_name(db0_fixture):
    prefix = db0.get_current_prefix()
    _ = MemoTestClass(123)
    assert len(list(db0.get_raw_memo_classes(prefix.name))) > 0


def test_get_memo_classes_by_current_prefix(db0_fixture):
    prefix = db0.get_current_prefix()
    _ = MemoTestClass(123)
    assert len(list(db0.get_memo_classes(prefix))) > 0


def test_get_raw_memo_classes_by_prefix_uuid(db0_fixture):
    _ = MemoTestClass(123)
    count = sum([len(list(db0.get_raw_memo_classes(prefix_uuid=uuid))) for _, uuid in db0.get_prefixes()])
    assert count > 0


def test_get_raw_memo_classes_by_prefix_name_and_uuid(db0_fixture):
    _ = MemoTestClass(123)
    count = sum([len(list(db0.get_raw_memo_classes(prefix_name=name, prefix_uuid=uuid))) for name, uuid in db0.get_prefixes()])
    assert count > 0


def test_get_raw_memo_classes_raises_when_mismatched_name_and_uuid(db0_fixture):
    _ = MemoTestClass(123)
    with pytest.raises(Exception):
        _ = [list(db0.get_raw_memo_classes(prefix_name=name, prefix_uuid=123)) for name, _ in db0.get_prefixes()]
    
        
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
    memo_type = [obj for obj in db0.get_memo_classes()][0].get_class()
    # find all objects of this type
    assert len(list(db0.find(memo_type))) == 1
    
    
def test_get_instance_count_for_class(db0_fixture):
    _ = MemoTestClass(123)
    memo_info = [obj for obj in db0.get_memo_classes() if not obj.is_singleton][0]
    assert memo_info.get_instance_count() > 0


def test_memo_class_get_attribute_values(db0_fixture):
    def values_of(obj, attr_names):
        return [getattr(obj, attr_name) for attr_name in attr_names]
    
    db0.tags(MemoTestClass(datetime.now()), MemoTestClass(123)).add("tag1")
    memo_info = [obj for obj in db0.get_memo_classes() if not obj.is_singleton][0]
    attr_names = [attr.name for attr in memo_info.get_attributes()]
    for obj in db0.find(MemoTestClass):
        assert len(values_of(obj, attr_names)) == len(attr_names)
        
    
def test_get_attributes_by_type(db0_fixture):
    obj = MemoTestClass(123)    
    assert len(list(db0.get_attributes(type(obj)))) > 0


def test_get_memo_class_by_uuid(db0_fixture):
    _ = MemoTestClass(123)
    meta_1 = list(db0.get_memo_classes())[0]
    meta_2 = db0.get_memo_class(meta_1.class_uuid)
    assert meta_1 == meta_2
    
    
def test_get_methods(db0_fixture):
    obj = MemoTestClassWithMethods(123)    
    methods = list(db0.get_methods(obj))
    assert len(methods) == 3    
    
    
def test_get_all_instances_of_known_type_from_snapshot(db0_fixture, memo_tags):
    # commit to make data available to snapshot
    db0.commit()
    with db0.snapshot() as snap:
        total_len = 0
        for memo_class in db0.get_memo_classes():
            uuids = [db0.uuid(obj) for obj in memo_class.all(snap)]
            total_len += len(uuids)
        assert total_len > 0


def test_get_all_instances_of_unknown_type_from_snapshot(db0_fixture, memo_tags):
    db0.commit()
    with db0.snapshot() as snap:
        total_len = 0
        for memo_class in db0.get_memo_classes():
            uuids = [db0.uuid(obj) for obj in memo_class.all(snapshot=snap, as_memo_base=True)]
            total_len += len(uuids)
        assert total_len > 0

    
def test_import_model(db0_fixture):
    db0.import_model("datetime")