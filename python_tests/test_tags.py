import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestSingleton, MemoTestClass


@db0.memo()
class MemoClassForTags:
    def __init__(self, value):
        self.value = value
    

@db0.memo()
class DifferentClassForTags:
    def __init__(self, value):
        self.value = value


def test_assign_single_tag_to_memo_object(db0_fixture):
    object_1 = MemoClassForTags(1)
    root = MemoTestSingleton(object_1)
    db0.tags(object_1).add("tag1")


def test_find_by_single_tag(db0_fixture):
    object_1 = MemoClassForTags(1)
    root = MemoTestSingleton(object_1)
    # assign tag first
    db0.tags(object_1).add("tag1")
    # then try looking up by the assigned tag
    assert len(list(db0.find("tag1"))) == 1
    assert len(list(db0.find("tag2"))) == 0


def test_assign_multiple_tags_as_varargs(db0_fixture):
    object_1 = MemoClassForTags(1)
    root = MemoTestSingleton(object_1)
    # assign multiple tags
    db0.tags(object_1).add("tag1", "tag2")
    assert len(list(db0.find("tag1"))) == 1
    assert len(list(db0.find("tag2"))) == 1
    assert len(list(db0.find("tag3"))) == 0


def test_assign_tags_as_values_with_operator(db0_fixture):
    object_1 = MemoClassForTags(1)
    root = MemoTestSingleton(object_1)
    # assign multiple tags
    tags = db0.tags(object_1)
    tags  += "tag1"
    tags  += "tag2"
    assert len(list(db0.find("tag1"))) == 1
    assert len(list(db0.find("tag2"))) == 1
    assert len(list(db0.find("tag3"))) == 0


def test_assign_multiple_tags_as_list(db0_fixture):
    object_1 = MemoClassForTags(1)
    root = MemoTestSingleton(object_1)
    # assign multiple tags
    db0.tags(object_1).add(["tag1", "tag2"])
    assert len(list(db0.find("tag1"))) == 1
    assert len(list(db0.find("tag2"))) == 1
    assert len(list(db0.find("tag3"))) == 0


def test_assign_multiple_tags_as_list_with_operator(db0_fixture):
    object_1 = MemoClassForTags(1)
    root = MemoTestSingleton(object_1)
    # assign multiple tags
    tags = db0.tags(object_1)
    tags  += ["tag1", "tag2"]
    assert len(list(db0.find("tag1"))) == 1
    assert len(list(db0.find("tag2"))) == 1
    assert len(list(db0.find("tag3"))) == 0


def test_object_gets_incref_by_tags(db0_fixture):
    object_1 = MemoClassForTags(1)
    assert db0.getrefcount(object_1) == 0
    db0.tags(object_1).add(["tag1"])
    # commit to reflect tag assignment
    db0.commit()
    # ref-count is 2 because type tag is also assigned
    assert db0.getrefcount(object_1) == 2


def test_assigned_tags_can_be_removed(db0_fixture):
    object_1 = MemoClassForTags(1)
    db0.tags(object_1).add(["tag1", "tag2"])
    db0.tags(object_1).remove("tag1")
    assert len(list(db0.find("tag1"))) == 0
    assert len(list(db0.find("tag2"))) == 1
    db0.tags(object_1).remove("tag2")
    assert len(list(db0.find("tag2"))) == 0


def test_assigned_tags_can_be_removed_with_operators(db0_fixture):
    object_1 = MemoClassForTags(1)
    tags = db0.tags(object_1)
    tags.add(["tag1", "tag2"])
    tags -= "tag1"
    assert len(list(db0.find("tag1"))) == 0
    assert len(list(db0.find("tag2"))) == 1
    tags -= "tag2"
    assert len(list(db0.find("tag2"))) == 0


def test_assigned_tags_can_be_removed_as_list_with_operators(db0_fixture):
    object_1 = MemoClassForTags(1)
    tags = db0.tags(object_1)
    tags.add(["tag1", "tag2"])
    assert len(list(db0.find("tag1"))) == 1
    assert len(list(db0.find("tag2"))) == 1
    tags -= ["tag1", "tag2"]
    assert len(list(db0.find("tag1"))) == 0
    assert len(list(db0.find("tag2"))) == 0


# def test_object_gets_dropped_if_norefs_after_tags_removed(db0_fixture):
#     object_1 = MemoClassForTags(1)
#     uuid = db0.uuid(object_1)
#     db0.tags(object_1).add(["tag1", "tag2"])
#     db0.commit()
#     # remove tags
#     db0.tags(object_1).remove(["tag1", "tag2"])
#     db0.commit()
#     del object_1
#     # object should be dropped from DBZero
#     with pytest.raises(Exception):
#         db0.fetch(uuid)


def test_find_by_tag_and_type(db0_fixture):
    object_1 = MemoClassForTags(0)
    object_2 = DifferentClassForTags(object_1)
    object_3 = DifferentClassForTags(object_2)
    root = MemoTestSingleton(object_3)
    db0.tags(object_1).add("tag1")
    db0.tags(object_2).add("tag1")
    db0.tags(object_3).add("tag1")
    # look up by tag and type
    assert len(list(db0.find(MemoClassForTags, "tag1"))) == 1
    assert len(list(db0.find(DifferentClassForTags, "tag1"))) == 2


def test_tags_can_be_applied_to_multiple_objects(db0_fixture):
    objects = [MemoClassForTags(i) for i in range(3)]    
    db0.tags(objects[0], objects[1], objects[2]).add("tag1")
    # look up by tag and type
    assert len(list(db0.find("tag1"))) == 3


def test_tag_queries_can_use_or_filters(db0_fixture):
    objects = [MemoClassForTags(i) for i in range(10)]
    db0.tags(objects[4]).add(["tag1", "tag2"])
    db0.tags(objects[6]).add(["tag1", "tag3"])
    db0.tags(objects[2]).add(["tag3", "tag4"])
    
    values = set([x.value for x in db0.find(["tag1", "tag4"])])
    assert values == set([2, 4, 6])


def test_tag_queries_can_use_no_operator(db0_fixture):
    objects = [MemoClassForTags(i) for i in range(10)]
    db0.tags(objects[4]).add(["tag1", "tag2"])
    db0.tags(objects[6]).add(["tag4", "tag3"])
    db0.tags(objects[2]).add(["tag3", "tag4"])
    
    values = set([x.value for x in db0.find(MemoClassForTags, db0.no("tag1"))])
    assert values == set([2, 6])


def test_tuple_can_be_used_for_tag_search(db0_fixture):
    objects = [MemoClassForTags(i) for i in range(10)]
    db0.tags(objects[4]).add(["tag1", "tag2"])
    db0.tags(objects[6]).add(["tag4", "tag3"])
    db0.tags(objects[2]).add(["tag3", "tag4"])
        
    values = set([x.value for x in db0.find(MemoClassForTags, ("tag4", "tag3"))])
    assert values == set([2, 6])


def test_memo_instance_can_be_used_as_tag(db0_fixture):
    root = MemoTestSingleton(0)
    objects = [MemoClassForTags(i) for i in range(10)]
    db0.tags(objects[4]).add(root)
    assert db0.is_tag(root)


def test_find_by_memo_instance_as_tag(db0_fixture):
    # make 2 instances to be used as tags
    tags = [MemoClassForTags(i) for i in range(3)]
    objects = [MemoClassForTags(i) for i in range(10)]
    db0.tags(objects[4]).add([tags[0], tags[1]])
    db0.tags(objects[6]).add([tags[1], tags[2]])
    db0.tags(objects[2]).add([tags[0], tags[2]])
    
    values = set([x.value for x in db0.find(MemoClassForTags, tags[0])])
    assert values == set([2, 4])


def test_typed_find_with_string_tags(db0_fixture):
    objects = [MemoTestClass(i) for i in range(10)]
    db0.tags(objects[4]).add("one")
    db0.tags(objects[6]).add("two")
    db0.tags(objects[2]).add("one")
    
    assert len(list(db0.find(MemoClassForTags, "one"))) == 0
    values = set([x.value for x in db0.find(MemoTestClass, "one")])
    assert values == set([2, 4])


def test_remove_tags_then_find_typed(db0_fixture):
    objects = [MemoTestClass(i) for i in range(10)]
    db0.tags(objects[4]).add("one")
    db0.tags(objects[6]).add("two")
    db0.tags(objects[2]).add("one")
    
    db0.tags(objects[4], objects[2]).remove("one")    
    assert len(list(db0.find(MemoTestClass, "one"))) == 0


def test_tags_can_be_assigned_on_empty_list(db0_fixture):
    objects = [MemoTestClass(i) for i in range(10)]
    db0.tags(*[]).add("one")
    assert len(list(db0.find(MemoClassForTags, "one"))) == 0


def test_assign_tags_in_multiple_operations(db0_fixture):
    for x in range(2):
        for i in range(3):
            obj = MemoTestClass(i)            
            db0.tags(obj).add("tag1")
                
        count = len(list(db0.find("tag1")))
        assert count == 3 * (x + 1)


def test_query_by_non_existing_tag(db0_fixture):
    assert len(list(db0.find("tag1"))) == 0


def test_query_by_removed_tag_issue_1(db0_fixture):
    """
    This test was failing with slab does not exist exception
    (accessing invalid address 0x0)
    """
    buf = []
    for i in range(10):
        obj = MemoTestClass(i)
        db0.tags(obj).add(["tag1", "tag2"])
        buf.append(obj)
    
    db0.tags(*buf).remove("tag1")
    assert len(list(db0.find("tag1"))) == 0


def test_mutating_tags_while_running_query_from_snapshot(db0_fixture):
    for i in range(10):
        obj = MemoTestClass(i)
        db0.tags(obj).add(["tag1", "tag2"])
    db0.commit()
    # run query over a snapshot while updating tags
    count = 0
    for snaphot_obj in db0.snapshot().find(("tag1", "tag2")):
        # NOTE: since snapshot objects are immutable we need to fetch the object from
        # the head transaction to mutate it
        if count % 2 == 0:
            obj = db0.fetch(db0.uuid(snaphot_obj))        
            db0.tags(obj).remove("tag1")
        count += 1
    
    assert count == 10
    assert len(list(db0.find("tag1"))) == 5
    