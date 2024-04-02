import pytest
import dbzero_ce as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton
from dbzero_ce import find


def test_index_instance_can_be_created_without_arguments(db0_fixture):
    index = db0.index()
    assert index is not None
    
    
def test_can_add_elements_to_index(db0_fixture):
    index = db0.index()
    # key, value
    index.add(1, MemoTestClass(999))
    assert len(index) == 1
    

def test_index_updates_are_flushed_on_commit(db0_fixture):
    index = db0.index()
    prefix_name = db0.get_current_prefix()
    uuid = db0.uuid(index)
    root = MemoTestSingleton(index)
    # key, value
    index.add(1, MemoTestClass(999))
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(prefix_name, "r")    
    index = db0.fetch(uuid)
    assert len(index) == 1


def test_range_index_can_sort_results_of_tags_query(db0_fixture):
    index = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [999, 666, 555, 888, 777]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # key, value
        index.add(priority[i], objects[i])
    
    assert len(index) == 5
    # retrieve sorted elements using index
    sorted = index.sort(find("tag1"))
    values = [x.value for x in sorted]
    assert values == [2, 1, 4, 3, 0]


def test_range_index_can_store_nulls(db0_fixture):
    index = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [666, None, 555, 888, None]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # key, value
        index.add(priority[i], objects[i])
    
    assert len(index) == 5
    # retrieve sorted elements using index
    sorted = index.sort(find("tag1"))
    values = [x.value for x in sorted]
    assert values == [2, 0, 3, 4, 1]


def test_range_index_can_sort_by_multiple_criteria(db0_fixture):
    index_1 = db0.index()
    index_2 = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [(666, 999), (999, 666), (666, 555), (888, 888), (999, 777)]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # key, value
        index_1.add(priority[i][0], objects[i])
        index_2.add(priority[i][1], objects[i])
        
    # retrieve elements sorted by multiple criteria
    sorted = index_1.sort(index_2.sort(find("tag1")))
    values = [x.value for x in sorted]
    assert values == [2, 0, 3, 1, 4]


def test_range_index_can_sort_by_multiple_criteria_with_nulls(db0_fixture):
    index_1 = db0.index()
    index_2 = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [(666, 999), (None, 666), (666, 555), (888, 888), (None, 777)]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # key, value
        index_1.add(priority[i][0], objects[i])
        index_2.add(priority[i][1], objects[i])
    
    # retrieve elements sorted by multiple criteria
    sorted = index_1.sort(index_2.sort(find("tag1")))
    values = [x.value for x in sorted]
    assert values == [2, 0, 3, 1, 4]


def test_index_can_be_class_member(db0_fixture):
    # index put as a class member
    object = MemoTestClass(db0.index())
    object.value.add(1, object)
    assert len(object.value) == 1    


def test_range_index_can_evaluate_range_query(db0_fixture):
    index_1 = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [666, 22, 99, 888, 444]
    for i in range(5):
        # key, value
        index_1.add(priority[i], objects[i])
    
    # retrieve specific range of keys
    range_query = index_1.range(50, 700)
    values = set([x.value for x in range_query])
    assert values == set([0, 2, 4])


def test_range_and_tag_filters_can_be_combined(db0_fixture):
    ix_one = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [666, 22, 99, 888, 444]
    for i in range(5):
        # key, value
        ix_one.add(priority[i], objects[i])
        if i % 2 == 0:
            db0.tags(objects[i]).add(["tag1", "tag2"])
    
    # retrieve specific range of keys    
    values = set([x.value for x in db0.find("tag1", ix_one.range(99, 800))])
    assert values == set([0, 2, 4])


def test_range_index_query_is_inclusive_by_default(db0_fixture):
    index_1 = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [666, 22, 99, 888, 444]
    for i in range(5):
        # key, value
        index_1.add(priority[i], objects[i])
        
    values = set([x.value for x in index_1.range(22, 444)])
    assert values == set([1, 2, 4])
