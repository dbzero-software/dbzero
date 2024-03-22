import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestSingleton


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


def test_assign_multiple_tags_as_list(db0_fixture):
    object_1 = MemoClassForTags(1)
    root = MemoTestSingleton(object_1)
    # assign multiple tags
    db0.tags(object_1).add(["tag1", "tag2"])
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
