import pytest
import dbzero_ce as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoScopedClass
from dbzero_ce import find
from datetime import timedelta, datetime


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


def test_sorting_empty_tag_filter(db0_fixture):
    ix_one = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [666, 22, 99, 888, 444]
    for i in range(5):
        # key, value
        ix_one.add(priority[i], objects[i])
        if i % 2 == 0:
            db0.tags(objects[i]).add("tag1")
        else:
            db0.tags(objects[i]).add("tag2")
    
    assert len(list(find("tag1", "tag2"))) == 0
    assert len(list(ix_one.sort(find("tag1", "tag2")))) == 0


def test_range_index_can_sort_by_datetime(db0_fixture):
    index = db0.index()
    dt_base = datetime.now()
    objects = [MemoTestClass(dt_base + timedelta(seconds=i + 1)) for i in range(5)]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # datetime key, value
        index.add(objects[i].value, objects[i])
    
    result = list(index.sort(db0.find("tag1")))
    last_value = dt_base
    for object in result:
        assert object.value >= last_value
        last_value = object.value


def test_index_can_hold_all_null_elements(db0_fixture):
    index = db0.index()
    # key, value
    for _ in range(5):
        index.add(None, MemoTestClass(999))
    assert len(index) == 5


def test_index_can_add_non_null_after_adding_null_first(db0_fixture):
    index = db0.index()
    for _ in range(5):
        # add null elements only
        index.add(None, MemoTestClass(999))
    # extend the index with a non-null element
    index.add(1, MemoTestClass(999))
    assert len(index) == 6


def test_index_can_add_datetime_after_adding_null_first(db0_fixture):
    index = db0.index()
    for _ in range(5):
        # add null elements only
        index.add(None, MemoTestClass(999))
    # extend the index with a non-null element
    index.add(datetime.now(), MemoTestClass(999))
    assert len(index) == 6


def test_index_can_sort_all_null_values(db0_fixture):
    index = db0.index()
    for i in range(3):
        # add null elements only
        object = MemoTestClass(i)
        db0.tags(object).add("tag1")
        index.add(None, object)
    
    values = set([x.value for x in index.sort(find("tag1"))])
    assert values == set([0, 1, 2])


def test_low_unbounded_range_query(db0_fixture):
    index = db0.index()
    for i in range(10):
        # add null elements only
        index.add(i, MemoTestClass(i))

    # run range query passing a concrete type
    values = set([x.value for x in index.range(None, 4)])
    assert values == set([0, 1, 2, 3, 4])


def test_high_unbounded_range_query(db0_fixture):
    index = db0.index()
    for i in range(10):
        # add null elements only
        index.add(i, MemoTestClass(i))

    # run range query passing a concrete type
    values = set([x.value for x in index.range(7, None)])
    assert values == set([7, 8, 9])


def test_both_side_unbounded_range_query(db0_fixture):
    index = db0.index()
    for i in range(5):
        # add null elements only
        index.add(i, MemoTestClass(i))

    # run range query passing a concrete type
    values = set([x.value for x in index.range(None, None)])
    assert values == set([0, 1, 2, 3, 4])


def test_null_index_can_run_query_with_incompatible_range_type(db0_fixture):
    index = db0.index()
    for i in range(3):
        # add null elements only
        index.add(None, MemoTestClass(i))
    
    # run range query passing a concrete type
    values = set([x.value for x in index.range(datetime.now(), None)])
    assert values == set([0, 1, 2])


def test_invalid_addr_case(db0_fixture):
    index = db0.index()
    for i in range(3):
        # add null elements only
        object = MemoTestClass(i)
        db0.tags(object).add("tag1")
        index.add(None, object)
    
    values = set([x.value for x in index.sort(find("tag1"))])
    assert values == set([0, 1, 2])


def test_null_first_range_query(db0_fixture):
    index = db0.index()
    # combine null + non-null elements
    index.add(None, MemoTestClass(999))
    for i in range(3):
        index.add(i, MemoTestClass(i))

    # null-first query should include null in the high-bound range
    values = set([x.value for x in index.range(None, 1 , null_first=True)])
    assert values == set([999, 0, 1])


def test_can_remove_elements_from_index(db0_fixture):
    index = db0.index()    
    obj_1 = MemoTestClass(999)
    # key, value
    index.add(1, obj_1)
    assert len(index) == 1
    db0.commit()
    # then remove (must pass an existing key and value)
    index.remove(1, obj_1)
    assert len(index) == 0


def test_adding_and_removing_index_elements_in_same_transaction(db0_fixture):
    index = db0.index()    
    obj_1, obj_2 = MemoTestClass(999), MemoTestClass(888)    
    index.add(1, obj_1)
    index.add(2, obj_2)
    index.remove(1, obj_1)
    db0.commit()
    assert len(index) == 1


def test_remove_then_add_element_to_index(db0_fixture):
    index = db0.index()
    obj_1 = MemoTestClass(999)
    index.remove(1, obj_1)
    index.add(1, obj_1)    
    assert len(index) == 1


def test_removing_null_keys_from_index(db0_fixture):
    index = db0.index()
    obj_1, obj_2, obj_3 = MemoTestClass(999), MemoTestClass(888), MemoTestClass(777)
    index.add(None, obj_1)
    index.add(None, obj_2)
    index.add(None, obj_3)
    assert len(index) == 3
    db0.commit()
    index.remove(None, obj_1)
    index.remove(None, obj_3)
    assert len(index) == 1


def test_index_sort_descending(db0_fixture):
    index = db0.index()
    priority = [666, None, 555, 888, None]
    objects = [MemoTestClass(i) for i in priority]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # key, value
        index.add(priority[i], objects[i])
    
    # retrieve sorted elements using index
    sorted = index.sort(find("tag1"), desc=True)
    values = [x.value for x in sorted]
    assert values == [None, None, 888, 666, 555]


def test_index_sort_asc_desc_with_null_first_policy(db0_fixture):
    index = db0.index()
    priority = [666, None, 555, 888, None]
    objects = [MemoTestClass(i) for i in priority]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # key, value
        index.add(priority[i], objects[i])
    
    assert [x.value for x in index.sort(find("tag1"), null_first=False)] == [555, 666, 888, None, None]
    assert [x.value for x in index.sort(find("tag1"), desc=True, null_first=False)] == [None, None, 888, 666, 555]
    assert [x.value for x in index.sort(find("tag1"), desc=True)] == [None, None, 888, 666, 555]
    assert [x.value for x in index.sort(find("tag1"), null_first=True)] == [None, None, 555, 666, 888]


def test_scoped_datetime_index_issue(db0_fixture):
    prefix = "test-data"
    obj = MemoScopedClass(db0.index(), prefix=prefix)    
    index = obj.value
    index.add(datetime.now(), MemoScopedClass(100, prefix=prefix))
    assert len(list(index.range(None, None, null_first=True))) == 1
