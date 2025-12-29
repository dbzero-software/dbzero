# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .conftest import DB0_DIR
from .memo_test_types import MemoTestClass, MemoTestSingleton, MemoScopedClass, MemoScopedSingleton
from dbzero import find
from datetime import timedelta, datetime
import random
import time
from .conftest import TEST_FILES_DIR_ROOT


def test_index_instance_can_be_created_without_arguments(db0_fixture):
    index = db0.index()
    assert index is not None
    
    
def test_can_add_elements_to_index(db0_fixture):
    index = db0.index()
    # key, value
    index.add(1, MemoTestClass(999))
    assert len(index) == 1
    

def test_index_updates_are_flushed_on_commit(db0_fixture):
    root = MemoTestSingleton(db0.index())
    index = root.value
    prefix = db0.get_current_prefix()
    uuid = db0.uuid(root)
    # key, value
    index.add(1, MemoTestClass(999))
    db0.commit()
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(prefix.name, "r")
    index = db0.fetch(uuid).value
    assert len(index) == 1


def test_index_updates_are_flushed_on_close(db0_fixture):
    obj = MemoTestClass(db0.index())
    index = obj.value
    prefix = db0.get_current_prefix()
    uuid = db0.uuid(obj)
    index.add(1, MemoTestClass(999))
    # NOTE: index not getting destroyed because Python instance is still alive
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(prefix.name, "r")
    index = db0.fetch(uuid).value
    assert len(index) == 1


def test_index_can_sort_results_of_tags_query(db0_fixture):
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


def test_index_can_store_nulls(db0_fixture):
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


def test_index_can_sort_by_multiple_criteria(db0_fixture):
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


def test_index_can_sort_by_multiple_criteria_with_nulls(db0_fixture):
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


def test_index_can_evaluate_select_query(db0_fixture):
    index_1 = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [666, 22, 99, 888, 444]
    for i in range(5):
        # key, value
        index_1.add(priority[i], objects[i])
    
    # retrieve specific range of keys (unsorted)
    select_query = index_1.select(50, 700)
    values = set([x.value for x in select_query])
    assert values == set([0, 2, 4])


def test_select_and_tag_filters_can_be_combined(db0_fixture):
    ix_one = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [666, 22, 99, 888, 444]
    for i in range(5):
        # key, value
        ix_one.add(priority[i], objects[i])
        if i % 2 == 0:
            db0.tags(objects[i]).add(["tag1", "tag2"])
    
    # retrieve specific range of keys    
    values = set([x.value for x in db0.find("tag1", ix_one.select(99, 800))])
    assert values == set([0, 2, 4])


def test_select_index_query_is_inclusive_by_default(db0_fixture):
    index_1 = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [666, 22, 99, 888, 444]
    for i in range(5):
        # key, value
        index_1.add(priority[i], objects[i])
        
    values = set([x.value for x in index_1.select(22, 444)])
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


def test_index_can_sort_by_datetime(db0_fixture):
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


def test_index_can_sort_by_datetime_between_years(db0_fixture):
    index = db0.index()
    dt_base = datetime.now()
    objects = [MemoTestClass(dt_base + timedelta(days=(i + 1)*365)) for i in range(5)]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # datetime key, value
        index.add(objects[i].value, objects[i])
    
    result = list(index.sort(db0.find("tag1")))
    last_value = dt_base
    for object in result:
        assert object.value >= last_value
        last_value = object.value


def test_index_can_sort_by_date(db0_fixture):
    index = db0.index()
    dt_base = datetime.now().date()
    objects = [MemoTestClass((dt_base + timedelta(days=i + 1))) for i in range(5)]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # datetime key, value
        index.add(objects[i].value, objects[i])
    
    result = list(index.sort(db0.find("tag1")))
    last_value = dt_base
    count = 0
    for object in result:
        assert object.value >= last_value
        last_value = object.value
        count += 1
    assert count == 5


def test_index_can_sort_by_date_between_years(db0_fixture):
    index = db0.index()
    dt_base = datetime.now().date()
    objects = [MemoTestClass((dt_base + timedelta(days=(i + 1)*365))) for i in range(5)]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # datetime key, value
        index.add(objects[i].value, objects[i])
    
    result = list(index.sort(db0.find("tag1")))
    last_value = dt_base
    count = 0
    for object in result:
        assert object.value >= last_value
        last_value = object.value
        count += 1
    assert count == 5

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


def test_low_unbounded_select_query(db0_fixture):
    index = db0.index()
    for i in range(10):
        # add null elements only
        index.add(i, MemoTestClass(i))

    # run range query passing a concrete type
    values = set([x.value for x in index.select(None, 4)])
    assert values == set([0, 1, 2, 3, 4])


def test_high_unbounded_select_query(db0_fixture):
    index = db0.index()
    for i in range(10):
        # add null elements only
        index.add(i, MemoTestClass(i))

    # run range query passing a concrete type
    values = set([x.value for x in index.select(7, None)])
    assert values == set([7, 8, 9])


def test_both_side_unbounded_select_query(db0_fixture):
    index = db0.index()
    for i in range(5):
        # add null elements only
        index.add(i, MemoTestClass(i))

    # run range query passing a concrete type
    values = set([x.value for x in index.select(None, None)])
    assert values == set([0, 1, 2, 3, 4])


def test_null_index_can_run_query_with_incompatible_select_type(db0_fixture):
    index = db0.index()
    for i in range(3):
        # add null elements only
        index.add(None, MemoTestClass(i))
    
    # run range query passing a concrete type
    values = set([x.value for x in index.select(datetime.now(), None)])
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


def test_null_first_select_query(db0_fixture):
    index = db0.index()
    # combine null + non-null elements
    index.add(None, MemoTestClass(999))
    for i in range(3):
        index.add(i, MemoTestClass(i))

    # null-first query should include null in the high-bound range
    values = set([x.value for x in index.select(None, 1, null_first=True)])
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
    assert len(list(index.select(None, None, null_first=True))) == 1


def test_select_query_on_empty_index(db0_fixture):
    index = db0.index()
    assert len(list(index.select(1, 2))) == 0


def test_select_query_on_empty_index_using_non_default_select_type(db0_fixture):
    index = db0.index()
    assert len(list(index.select(None, datetime.now()))) == 0


def test_unflushed_index_data_is_discarded_when_destroyed(db0_fixture):
    """
    This test was initially failing due to a non-virtual destructor not being invoked
    from the ObjectBase class
    """
    index = db0.index()
    index.add(1, MemoTestClass(999))
    del index
    # unreferenced index instance destroyed on close
    db0.close()


def test_unflushed_index_data_is_discarded_when_destroyed_before_close(db0_fixture):
    """
    This test was initially failing due to a non-virtual destructor not being invoked
    from the ObjectBase class
    """
    index = db0.index()
    index.add(1, MemoTestClass(999))
    del index
    db0.close()


# def test_moved_index_updates_are_flushed_on_close(db0_fixture):
#     prefix = db0.get_current_prefix()
#     # index instance moved from default prefix
#     root = MemoScopedSingleton(db0.index(), prefix="some-other-prefix")
#     root.value.add(1, MemoTestClass(999))
#     db0.close()
    
#     db0.init(DB0_DIR)
#     db0.open(prefix.name, "r")
#     db0.open("some-other-prefix", "rw")
#     root = MemoScopedSingleton(prefix="some-other-prefix")    
#     assert len(root.value) == 1


def test_index_unbounded_select_query(db0_fixture):
    # 2-side unbounded range query
    index = db0.index()
    for i in range(10):
        index.add(i, MemoTestClass(i))
    values = set([x for x in index.select(None, None)])
    assert len(values) == 10


def test_index_default_select_query(db0_fixture):
    # a default range query should return all elements (unbounded)
    index = db0.index()
    for i in range(10):
        index.add(i, MemoTestClass(i))
    values = set([x for x in index.select()])
    assert len(values) == 10


def test_index_destroys_its_depencencies_when_dropped(db0_fixture):
    index = db0.index()
    index.add(1, MemoTestClass(999))
    index.add(None, MemoTestClass(999))
    dep_uuids = []
    for obj in index.select(None, None):
        dep_uuids.append(db0.uuid(obj))
        # NOTE: important to drop python reference to obj otherwise will be accessible outside of the scope
        del obj
    db0.delete(index)
    del index    
    db0.commit()
    # make sure dependent instances has been destroyed as well
    for dep_uuid in dep_uuids:
        with pytest.raises(Exception):
            db0.fetch(dep_uuid)
        

def test_unflushed_index_destroys_its_depencencies_when_dropped(db0_fixture):    
    index = db0.index()
    obj = MemoTestClass(999)
    dep_uuid = db0.uuid(obj)
    index.add(1, obj)
    del obj
    # NOTE: at this point index is in the internally unflushed state
    # but its references should be accessible
    obj = db0.fetch(dep_uuid)
    del obj
    db0.delete(index)
    del index
    db0.commit()
    # make sure dependent instance has been destroyed as well
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)


def test_index_destroys_its_dependencies_when_removed(db0_fixture):
    index = db0.index()
    obj = MemoTestClass(999)
    dep_uuid = db0.uuid(obj)
    index.add(1, obj)
    del obj
    for obj in index.select(None, None):
        index.remove(1, obj)
        del obj
    
    db0.commit()
    # make sure dependent instance has been unreferenced
    with pytest.raises(Exception):
        db0.fetch(dep_uuid)


def test_index_same_object_under_multiple_keys(db0_fixture):
    index = db0.index()
    for x in range(5):
        obj = MemoTestClass(x)
        # NOTE: obj is inserted 10 times under different keys
        for key in range(10):
            index.add(key, obj)
    
    assert len(list(index.sort(index.select(None, 3), desc=True))) == len(list(index.select(None, 3)))
    
    
def test_index_add_remove_sequence_issue_1(db0_fixture):
    index = db0.index()
    # objects are not modified in this test
    obj_arr = [MemoTestClass(0) for _ in range(1000)]
    value_arr = [0] * 1000
    # NOTE: other sequences might not reproduce the issue
    data = [
        696, 496, 781, 777, 55, 469, 998, 6, 964, 490, 824, 722, 425, 424, 491, 397, 975, 594, 217, 195, 483, 
        56, 65, 828, 428, 472, 671, 616, 162, 996, 374, 296, 325, 254, 387, 299, 170, 832, 854, 15, 646, 959, 
        331, 775, 942, 753, 180, 338, 37, 221, 221, 316, 112, 402, 130, 687, 232, 978, 593, 388, 163, 144, 334, 
        26, 903, 161, 421, 694, 180, 503, 148, 965, 2, 831, 122, 773, 956, 843, 183, 866, 939, 310, 276, 634, 
        525, 244, 97, 110, 754, 5, 24, 33, 238, 503, 880, 967, 216, 872, 606, 491, 71, 242, 911, 244, 461, 870, 
        284, 993, 763, 565, 154, 794, 707, 142, 533, 624, 664, 20, 23, 818, 397, 588, 173, 425, 796, 559, 66, 
        310, 670, 632, 499, 29, 368, 504, 494, 160, 815, 467, 732, 354, 629, 644, 802, 733, 218, 564, 711, 698, 
        272, 370, 528, 170, 281, 602, 648, 72, 17, 480, 957, 628, 274, 311, 721, 545, 242, 936, 728, 754, 621, 
        594, 19, 869, 110, 296, 121, 48, 939, 144, 762, 875, 584, 54, 25, 367, 624, 346, 577, 451, 443, 265, 
        934, 529, 783, 919, 340, 352, 536, 135, 936, 572, 824, 583, 951, 118, 33, 127, 770, 522, 793, 539, 509, 
        984, 126, 548, 942, 482, 844, 300, 447, 472, 915, 409, 377, 48, 101, 989, 592, 989, 876, 909, 472, 969, 
        30, 864, 124, 885, 342, 874, 880, 78, 735, 9, 490, 739, 530, 118, 206, 307, 408, 470, 793, 945, 29, 232, 
        956, 829, 997, 611, 38, 167, 405, 407, 655, 275, 197, 644, 279, 400, 585, 236, 365, 191, 943, 671, 414, 
        441, 890, 212, 983, 830, 751, 145, 143, 782, 490, 768, 105, 510, 69, 997, 820, 78, 426, 163, 482, 732, 
        641, 619, 893, 185, 490, 8, 17, 376, 142, 980, 989, 568, 812, 602, 767, 159, 146, 749, 58, 438, 276, 
        490, 27, 270, 887, 763, 274, 218, 890, 218, 19, 679, 626, 386, 270, 855, 548, 264, 215, 521, 621, 326, 
        583, 807, 783, 452, 52, 113, 675, 815, 271, 840, 507, 26, 851, 628, 799, 544, 12, 818, 712, 755, 410, 
        882, 2, 315, 303, 874, 787, 85, 588, 535, 623, 966, 456, 841, 108, 434, 684, 417, 248, 704, 124, 317, 
        402, 67, 716, 50, 542, 889, 605, 828, 49, 883, 136, 609, 391, 697, 30, 521, 315, 890, 277, 53, 450, 808, 
        542, 910, 494, 697, 923, 953, 173, 759, 230, 807, 851, 408, 904, 995, 972, 386, 425, 115, 686, 915, 895
    ]
    
    nums = iter(data)
    # update objects with the index
    for a in range(4):
        for b in range(100):
            obj_num = next(nums)
            rand_obj = obj_arr[obj_num]
            if value_arr[obj_num] != 0:
                index.remove(value_arr[obj_num], rand_obj)
            value_arr[obj_num] = (a + 1) * (b + 1) * 123 % 1000
            index.add(value_arr[obj_num], rand_obj)
        db0.commit()
    
    # validate index with sort query
    assert len(list(index.sort(index.select(None, 500), desc=True))) == len(list(index.select(None, 500)))
    del obj_arr
    
    
def test_len_of_sorted_query(db0_fixture):
    index = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [999, 666, 555, 888, 777]
    for i in range(5):
        db0.tags(objects[i]).add(["tag1", "tag2"])
        # key, value
        index.add(priority[i], objects[i])
    
    query = index.sort(find("tag1"))
    assert len(query) == 5


def test_combine_multiple_select_queries_with_find(db0_fixture):
    ix_priority = db0.index()
    ix_date = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    priority = [999, 666, 555, 888, 777]
    dates = [datetime.now() + timedelta(seconds=i) for i in range(5)]
    for i in range(5):
        ix_priority.add(priority[i], objects[i])
        ix_date.add(dates[i], objects[i])
    
    query = db0.find(ix_priority.select(500, 800), ix_date.select(None, dates[3]))
    assert len(query) == 2


def test_find_in_index_select(db0_fixture):
    index = db0.index()
    index.add(1, MemoTestClass(1))
    index.add(2, MemoTestClass(2))
    test_obj = MemoTestClass(3)
    index.add(3, test_obj)
    assert test_obj in set(index.select())
    assert list(db0.find(index.select(), test_obj)) == [test_obj]


def test_find_multiple_objects_in_index_select(db0_fixture):
    index = db0.index()
    objects = [MemoTestClass(i) for i in range(5)]
    for obj in objects:
        if obj.value < 4:
            index.add(obj.value, obj)
    assert set(db0.find(index.select(), [objects[0], objects[3], objects[4]])) == set([objects[0], objects[3]])


@db0.memo
class TestSelfInsert():
    __test__ = False
    def __init__(self, v, index):
        index.add(v, db0.materialized(self))


def test_index_self_insert(db0_fixture):
    """
    Issue: the test was failing with MetaAllocator.cpp, line 263: Slab 4294426096 does not exist
    which is caused by a non-existing object (self accessed from __init__) added to the index
    Resolution: db0.materialized function added to db0
    """    
    index = db0.index()
    x = TestSelfInsert(1, index)
    index.select()


def test_index_sort_desc_null_last(db0_fixture):
    """
    Issue related with the following ticket:
    https://github.com/wskozlowski/dbzero/issues/281
    """
    index = db0.index()
    for p in [666, None, 555, 888, None]:
        obj = MemoTestClass(p)
        index.add(p, obj)
    
    assert [x.value for x in index.sort(index.select(), desc=False, null_first=True)] == [None, None, 555, 666, 888]
    assert [x.value for x in index.sort(index.select(), desc=False, null_first=False)] == [555, 666, 888, None, None]
    
    assert [x.value for x in index.sort(index.select(), desc=True, null_first=True)] == [888, 666, 555, None, None]
    assert [x.value for x in index.sort(index.select(), desc=True, null_first=False)] == [None, None, 888, 666, 555]
    
    
def test_index_default_sort_select_segv_issue_1(db0_fixture):
    index = db0.index()
    for p in [666, None, 555, 888, None]:
        obj = MemoTestClass(p)
        index.add(p, obj)

    assert [x.value for x in index.sort(index.select())] == [555, 666, 888, None, None]


def test_find_in_index_range_issue_1(db0_fixture):
    """
    Issue related with the following ticket:
    https://github.com/wskozlowski/dbzero/issues/239
    """    
    index = db0.index()
    index.add(1, MemoTestClass(1))
    index.add(2, MemoTestClass(2))
    test_obj = MemoTestClass(3)
    index.add(3, test_obj)
    assert test_obj in set(index.range())
    assert list(db0.find(index.range(), test_obj)) == [test_obj]
    
    
@pytest.mark.stress_test
def test_insert_1M_keys_to_index(db0_no_autocommit):
    cut = db0.index()
    objects = [MemoTestClass(0) for _ in range(25000)]
    # generate 1M random unique keys
    keys_list = random.sample(range(0, 100_000_000), 1_000_000)
    start = time.perf_counter()
    for i in range(1_000_000):
        # add random int
        cut.add(keys_list[i], random.choice(objects)) 
        if i % 10_000 == 0:
            assert len(cut) == i + 1
    result = list(cut.select(0, 1))
    end = time.perf_counter()
    assert len(cut) == 1_000_000
    print(f"Inserted 1M keys to index in {end - start:.2f} seconds")


@pytest.mark.stress_test
def test_insert_key_into_split_range (db0_no_autocommit):
    cut = db0.index()
    objects = []
    for i in range(35000):
        objects.append(MemoTestClass(i))
    start = time.perf_counter()
    elements =  257 * 1024
    # add more items than initial max_block_size to force block splits
    for i in range(0, elements):
        cut.add(i, objects[i % 35000])
        if i % 10000 == 0:
            print(f"Inserted {i} keys so far...")
            assert len(cut) == i + 1
        
    # add an item to bounded range that has been splitted
    cut.add(127, objects[-1])
    end = time.perf_counter()
    elements += 1
    assert len(cut) == elements
    print(f"Inserted {elements} keys to index in {end - start:.2f} seconds")
