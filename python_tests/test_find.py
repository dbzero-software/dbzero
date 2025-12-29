# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoDataPxClass, MemoTestSingleton, MemoClassForTags, MemoScopedClass
from .conftest import DB0_DIR, DATA_PX
import itertools
from datetime import datetime
import operator


@db0.memo()
class DifferentClassForTags:
    def __init__(self, value):
        self.value = value

@db0.memo
class MemoBaseClass:
    def __init__(self, value):
        self.value = value

@db0.memo
class MemoSecondSubClass(MemoBaseClass):
    def __init__(self, value):
        super().__init__(value)

@db0.memo
class MemoSubClass(MemoBaseClass):
    def __init__(self, value):
        super().__init__(value)

@db0.memo
class MemoSubSubClass(MemoSubClass):
    def __init__(self, value):
        super().__init__(value)

@db0.memo
class MemoErrorFromConstructor:
    def __init__(self, value):
        self.value = value
        db0.tags(self).add("tag1")
        raise Exception("Error from constructor")


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


def test_find_by_single_tag(db0_fixture):
    object_1 = MemoClassForTags(1)
    root = MemoTestSingleton(object_1)
    # assign tag first
    db0.tags(object_1).add("tag1")
    # then try looking up by the assigned tag
    assert len(list(db0.find("tag1"))) == 1
    assert len(list(db0.find("tag2"))) == 0

    
def test_tag_query_with_subquery(db0_no_autocommit, memo_tags):
    # combine the 2 queries
    query = db0.find(MemoTestClass, db0.find("tag1"))
    assert len(list(query)) == 10

    
def test_tuple_can_be_used_for_tag_search(db0_fixture):
    objects = [MemoClassForTags(i) for i in range(10)]
    db0.tags(objects[4]).add(["tag1", "tag2"])
    db0.tags(objects[6]).add(["tag4", "tag3"])
    db0.tags(objects[2]).add(["tag3", "tag4"])
            
    values = set([x.value for x in db0.find(MemoClassForTags, ("tag4", "tag3"))])
    assert values == set([2, 6])

    
def test_find_static_scoped_type(db0_fixture):
    px_name = db0.get_current_prefix().name
    db0.open(DATA_PX, "rw")
    # create scoped classes on data prefix
    for i in range(10):
        obj = MemoDataPxClass(i)
        db0.tags(obj).add("tag1")    
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(DATA_PX, "r")
    # change the default prefix
    db0.open(px_name, "r")
    # find class from a non-default prefix
    query = db0.find(MemoDataPxClass)
    assert len(list(query)) == 10
    
    
def test_tag_query_results_can_be_iterated_multiple_times(db0_no_autocommit, memo_tags):
    query = db0.find("tag1")
    l1 = len(list(query))
    l2 = len(list(query))
    assert l1 == l2
    

def test_using_len_to_determine_query_result_size(db0_no_autocommit, memo_tags):
    query = db0.find("tag1")
    assert len(query) == 10
    
    
def test_use_find_to_match_single_object(db0_no_autocommit, memo_tags):
    obj_1 = next(iter(db0.find("tag1")))
    assert len(db0.find(obj_1, db0.find("tag1"))) == 1
    

def test_find_base_type(db0_fixture):
    object_1 = MemoSubClass(1)
    # assign tag first
    db0.tags(object_1).add("tag1")
    # then try looking up by the assigned tag
    assert len(list(db0.find(MemoSubClass, "tag1"))) == 1
    assert len(list(db0.find(MemoBaseClass, "tag1"))) == 1


def test_find_base_type_multiple_subclass(db0_fixture):
    object_1 = MemoSubClass(1)
    object_2 = MemoSecondSubClass(2)
    object_3 = MemoSubSubClass(3)
    # assign tag first
    db0.tags(object_1).add("tag1")
    db0.tags(object_2).add("tag1")
    db0.tags(object_3).add("tag1")
    # then try looking up by the assigned tag
    assert len(list(db0.find(MemoSubClass, "tag1"))) == 2
    assert len(list(db0.find(MemoBaseClass, "tag1"))) == 3
    assert len(list(db0.find(MemoSecondSubClass, "tag1"))) == 1
    assert len(list(db0.find(MemoSubSubClass, "tag1"))) == 1


def test_tags_assigned_to_inherited_type_can_be_removed(db0_fixture):
    object_1 = MemoSubClass(1)
    db0.tags(object_1).add(["tag1", "tag2"])

    assert len(list(db0.find(MemoSubClass, "tag1"))) == 1
    assert len(list(db0.find(MemoBaseClass, "tag1"))) == 1
    assert len(list(db0.find(MemoSubClass, "tag2"))) == 1
    assert len(list(db0.find(MemoBaseClass, "tag2"))) == 1

    db0.tags(object_1).remove("tag1")
    assert len(list(db0.find(MemoSubClass, "tag1"))) == 0
    assert len(list(db0.find(MemoBaseClass, "tag1"))) == 0
    assert len(list(db0.find(MemoSubClass, "tag2"))) == 1
    assert len(list(db0.find(MemoBaseClass, "tag2"))) == 1

    db0.tags(object_1).remove("tag2")
    assert len(list(db0.find(MemoSubClass, "tag1"))) == 0
    assert len(list(db0.find(MemoBaseClass, "tag1"))) == 0
    assert len(list(db0.find(MemoSubClass, "tag2"))) == 0
    assert len(list(db0.find(MemoBaseClass, "tag2"))) == 0
    
    
def test_find_base_type_after_close(db0_fixture):
    prefix = db0_fixture.get_current_prefix()
    object_1 = MemoSubClass(1)
    db0.tags(object_1).add("tag1")
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix.name, "r")
    sublcass_list = list(db0.find(MemoBaseClass, "tag1"))
    assert len(sublcass_list) == 1
    assert sublcass_list[0].value == 1


def test_tagging_incomplete_objects(db0_fixture):
    obj = None
    try:
        obj = MemoErrorFromConstructor(1)
    except Exception:
        pass
    
    # object has NOT been created but what about tags ?
    assert obj is None    
    # no tags should be assigned either
    assert len(db0.find("tag1")) == 0
    db0.commit()
    assert len(db0.find("tag1")) == 0
    
    
def test_assign_multiple_tags_from_iterator(db0_fixture):
    object_1 = MemoClassForTags(1)
    db0.tags(object_1).add((tag for tag in ["tag1", "tag2", "tag3"]))
    result = list(db0.find("tag1", "tag2", "tag3"))
    assert len(result) == 1
    assert result[0].value == 1

    object_2 = MemoClassForTags(2)
    db0.tags(object_2).add(itertools.chain(["tag1", "tag2"], ["tag3", "tag4"]))
    result = list(db0.find("tag1", "tag2", "tag3", "tag4"))
    assert len(result) == 1
    assert result[0].value == 2

    d = {'tag5': 1, 'tag6': 2, 'tag7': 3}
    object_3 = MemoClassForTags(3)
    db0.tags(object_3).add(d)
    result = list(db0.find("tag5", "tag6", "tag7"))
    assert len(result) == 1
    assert result[0].value == 3
    
    
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
    with db0.snapshot() as snap:
        for snaphot_obj in snap.find(("tag1", "tag2")):
            # NOTE: since snapshot objects are immutable we need to fetch the object from
            # the head transaction to mutate it
            if count % 2 == 0:
                obj = db0.fetch(db0.uuid(snaphot_obj))        
                db0.tags(obj).remove("tag1")
            count += 1
    
    assert count == 10
    assert len(list(db0.find("tag1"))) == 5


def test_find_as_memo_base(db0_fixture, memo_tags):
    assert len(list(db0.find("tag1"))) > 0
    assert len(list(db0.find(db0.MemoBase, "tag1"))) == len(list(db0.find("tag1")))


def test_use_not_operator_with_find(db0_fixture, memo_tags):
    # yields some results
    assert db0.find("tag1")
    # yields no results
    assert not db0.find("no-such-tag")


def test_find_issue_1(db0_fixture):
    """
    Issue: https://github.com/wskozlowski/dbzero/issues/298
    """        
    query_object = MemoTestClass("test")
    query_object2 = MemoTestClass("test")
    db0.tags(query_object2).add(db0.as_tag(query_object))
    assert len(list(db0.find(query_object))) == 1
    
    
@db0.memo
class Attribute:
    def __init__(self, name, value):
        self.create_date = datetime.now()
        self.last_update = datetime.now()
        self.name = name
        self.value = value
        self.note_index = db0.index()
        self.tags = set()

    def tag_object(self, tags):
        for tag in tags:
            if tag not in self.tags:
                self.tags.add(tag)
                db0.tags(self).add(tag)
        self.update_time()

    def untag_object(self, tags):
        for tag in tags:
            if tag in self.tags:
                self.tags.remove(tag)
                db0.tags(self).remove(tag)
            self.update_time()

    def add_note(self, note):
        self.note_index.add(note.create_date, note)
        self.update_time()

    def update_time(self):
        self.last_update = datetime.now()
        
    
def test_tagging_and_untagging_in_single_commit_breaks_tag_search_issue2(db0_fixture):
    """
    Issue: https://github.com/wskozlowski/dbzero/issues/184
    """
    obj = Attribute("1", "1")
    assert obj.name == "1"
    assert obj.value == "1"
    obj.tag_object(["object", "tag1", "tag1_1"])
    old_tag_list = set(obj.tags)
    db0.commit()
    obj = [x for x in db0.find(Attribute, "object")][0]
    db0.commit()
    obj.untag_object(["object", "tag1", "tag1_1"])
    empty_tag_list = set(obj.tags)
    assert len(empty_tag_list) == 0
    obj.tag_object(["object", "tag1", "tag1_1"])
    db0.commit()
    new_tag_list = set(obj.tags)
    assert old_tag_list == new_tag_list
    objs = [x for x in db0.find(Attribute, "object")]
    assert len(objs) != 0
    
    
def test_find_with_scope_defined(db0_fixture):
    px_name = db0.get_current_prefix().name
    db0.tags(MemoScopedClass(123, prefix = px_name)).add("tag1")
    other_px_name = "other-px"
    db0.open(other_px_name)
    db0.tags(MemoScopedClass(456, prefix = other_px_name)).add("tag1")
    # use scoped find to find instances from different prefixes
    assert [x.value for x in db0.find(MemoScopedClass, "tag1", prefix = px_name)] == [123]
    assert [x.value for x in db0.find(MemoScopedClass, "tag1", prefix = other_px_name)] == [456]


def test_find_extract_multiple_indices(db0_no_autocommit, memo_tags):
    # pick elements by specific indexes
    result = db0.find("tag1")[3, 6, 7]
    assert type(result) is tuple
    assert len(result) == 3


def test_find_extract_duplicate_indices(db0_no_autocommit, memo_tags):
    # pick elements by specific indexes
    result = db0.find("tag1")[3, 6, 7, 3, 6]    
    assert result[0] is result[3]
    assert result[1] is result[4]
    
    
def test_find_extract_invalid_indices(db0_no_autocommit, memo_tags):
    query = db0.find("tag1")
    assert len(query) == 10
    with pytest.raises(IndexError):
        _ = db0.find("tag1")[3, 6, 7, 3, 10]
