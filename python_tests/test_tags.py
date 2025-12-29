# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import operator
import dbzero as db0
from .memo_test_types import MemoTestSingleton, MemoTestClass, MemoScopedClass, MemoClassForTags, MemoTestClassWithMethods, MemoNoDefTags
from .conftest import DB0_DIR, DATA_PX
import itertools

    
@db0.memo
class MemoWithTagsAssignedOnInit:
    def __init__(self, value, *tags):
        self.value = value
        db0.tags(self).add(*tags)

    
def test_assign_single_tag_to_memo_object(db0_fixture):
    object_1 = MemoClassForTags(1)
    root = MemoTestSingleton(object_1)
    db0.tags(object_1).add("tag1")


def test_object_gets_incref_by_tags(db0_fixture):
    object_1 = MemoClassForTags(1)
    assert db0.getrefcount(object_1) == 0
    db0.tags(object_1).add(["tag1"])
    # commit to reflect tag assignment
    db0.commit()    
    assert db0.getrefcount(object_1) == 1


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


def test_object_gets_dropped_if_norefs_after_tags_removed(db0_fixture):
    object_1 = MemoClassForTags(1)
    uuid = db0.uuid(object_1)
    db0.tags(object_1).add(["tag1", "tag2"])
    db0.commit()
    # remove tags
    db0.tags(object_1).remove(["tag1", "tag2"])
    del object_1    
    db0.commit()
    # object should be dropped from dbzero
    with pytest.raises(Exception):
        db0.fetch(uuid)


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
    db0.tags(objects[2]).add(["tag3", "tag1"])
    
    values = set([x.value for x in db0.find(MemoClassForTags, db0.no("tag1"))])
    assert values == {0, 1, 3, 5, 6, 7, 8, 9}


def test_memo_instance_can_be_used_as_tag(db0_fixture):
    root = MemoTestSingleton(0)
    objects = [MemoClassForTags(i) for i in range(10)]
    # we can use as_tag or the instance directly
    db0.tags(objects[4]).add(root, db0.as_tag(root))


def test_find_by_memo_instance_as_tag(db0_fixture):
    # make 2 instances to be used as tags
    tags = [db0.as_tag(MemoClassForTags(i)) for i in range(3)]
    objects = [MemoClassForTags(i) for i in range(10)]
    db0.tags(objects[4]).add([tags[0], tags[1]])
    db0.tags(objects[6]).add([tags[1], tags[2]])
    db0.tags(objects[2]).add([tags[0], tags[2]])

    values = set([x.value for x in db0.find(MemoClassForTags, tags[0])])
    assert values == set([2, 4])


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


def test_adding_tags_on_mixed_prefixes(db0_fixture):
    obj_1 = MemoTestClass(1)
    db0.open("my-other-prefix", "rw")
    obj_2 = MemoScopedClass(2, prefix = "my-other-prefix")
    
    # it's allowed to update both prefixes in one operation
    db0.tags(obj_1, obj_2).add("tag1")


def test_tags_can_be_assigned_on_init(db0_no_autocommit):
    obj = MemoWithTagsAssignedOnInit(1, "tag1", "tag2")
    # find by type
    assert len(list(db0.find(MemoWithTagsAssignedOnInit))) == 1
    assert len(list(db0.find("tag1"))) == 1
    assert len(list(db0.find("tag2"))) == 1
    
        
def test_tags_string_pool_storage(db0_fixture):
    sp_size_1 = db0.get_prefix_stats()["string_pool"]["size"]
    obj = MemoTestClass(0)
    db0.tags(obj).add(["completely-new-tag"])
    # commit to flush updates
    db0.commit()
    sp_size_2 = db0.get_prefix_stats()["string_pool"]["size"]
    assert sp_size_2 > sp_size_1


def test_unused_tags_removed_from_string_pool(db0_fixture):    
    obj = MemoTestClass(0)
    db0.tags(obj).add(["completely-new-tag"])
    # commit to flush updates
    db0.commit()
    sp_size_1 = db0.get_prefix_stats()["string_pool"]["size"]
    db0.tags(obj).remove(["completely-new-tag"])
    db0.commit()
    sp_size_2 = db0.get_prefix_stats()["string_pool"]["size"]
    # make sure tag was removed from string pool
    assert sp_size_2 < sp_size_1


def test_tags_compare(db0_fixture):
    obj1 = MemoClassForTags(1)
    obj2 = MemoClassForTags(2)
    tag1 = db0.as_tag(obj1)
    tag2 = db0.as_tag(obj2)

    assert tag1 == tag1
    assert tag1 == db0.as_tag(obj1)
    assert (tag1 != db0.as_tag(obj1)) == False

    assert tag1 != tag2
    assert (tag1 == tag2) == False

    for op in (operator.lt, operator.le, operator.ge, operator.gt):
        with pytest.raises(TypeError):
            op(tag1, tag1)
        with pytest.raises(TypeError):
            op(tag1, tag2)
    
    assert tag1 != 'test'
    assert (tag1 == 'test') == False
    assert tag1 != 123
    assert tag1 != None
    assert tag1 != []


def test_tags_set_operations(db0_fixture):
    obj1 = MemoClassForTags(1)
    tag1 = db0.as_tag(obj1)
    assert hash(tag1) == hash(tag1)
    assert hash(tag1) == hash(db0.as_tag(obj1))

    obj2 = MemoClassForTags(2)
    tag2 = db0.as_tag(obj2)
    assert hash(tag1) != hash(tag2)

    result = set((tag1, db0.as_tag(obj1), db0.as_tag(obj1)))
    assert len(result) == 1
    assert list(result) == [tag1]

    obj3 = MemoClassForTags(3)
    tag3 = db0.as_tag(obj3)

    set1 = set((tag1, tag2))
    set2 = set((tag2, tag3))

    set_sum = set1 | set2
    assert all((tag in set_sum for tag in (tag1, tag2, tag3)))
    assert list(set1 - set2) == [tag1]
    assert list(set2 - set1) == [tag3]


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


def test_tag_remove_then_add_in_single_transaction(db0_fixture):
    obj = MemoTestClass(0)
    db0.tags(obj).add("object")
    db0.commit()
    db0.tags(obj).remove("object")
    db0.tags(obj).add("object")
    db0.commit()
    objs = [x for x in db0.find(MemoTestClass, "object")]
    assert len(objs) > 0


@pytest.mark.stress_test
def test_add_250k_tags_low_cache(db0_no_autocommit):
    # limit cache size for cache recycler testing
    db0.set_cache_size(256 << 10)
    def random_tag(max_length=24):
        import random
        import string
        return ''.join(random.choices(string.ascii_letters + string.digits, k=max_length))
    
    obj = MemoTestClass(0)
    for i in range(100):
        last_tags = [random_tag() for _ in range(1000)]
        db0.tags(obj).add(last_tags)        
        print(f"Next batch done: {i + 1}000 tags added")
        
    db0.commit()
    
    for tag in last_tags:
        assert len(list(db0.find(tag))) == 1, f"Tag {tag} not found after adding 250k tags"

    
def test_object_with_no_refs_destroyed_when_last_tag_removed(db0_fixture):
    db0.tags(MemoTestClass(0)).add("tag-1")
    uuid = db0.uuid(next(iter(db0.find(MemoTestClass, "tag-1"))))
    db0.commit()
    assert db0.exists(uuid) 
    db0.tags(next(iter(db0.find(MemoTestClass, "tag-1")))).remove("tag-1")
    db0.commit()
    assert not db0.exists(uuid)


def test_object_with_no_refs_cannot_be_fetched_when_last_tag_removed(db0_fixture):
    db0.tags(MemoTestClass(0)).add("tag-1")
    uuid = db0.uuid(next(iter(db0.find(MemoTestClass, "tag-1"))))
    db0.commit()
    db0.tags(next(iter(db0.find(MemoTestClass, "tag-1")))).remove("tag-1")
    db0.commit()
    with pytest.raises(Exception):
        _ = db0.fetch(uuid)
    
    
def test_object_with_no_refs_no_longer_accessible_by_type_when_last_tag_removed(db0_fixture):
    db0.tags(MemoTestClass(0)).add("tag-1")
    uuid = db0.uuid(next(iter(db0.find(MemoTestClass, "tag-1"))))
    db0.commit()    
    db0.tags(next(iter(db0.find(MemoTestClass, "tag-1")))).remove("tag-1")
    assert not db0.find(MemoTestClass)


def test_type_tags_automatically_assigned(db0_fixture):
    obj_1 = MemoTestClass(0)
    assert len(db0.find(MemoTestClass)) == 1


def test_deleted_object_cannot_be_looked_up_by_type(db0_fixture):
    obj_1 = MemoTestClass(0)
    del obj_1    
    assert len(db0.find(MemoTestClass)) == 0


def test_object_persisted_when_tag_assigned(db0_fixture):
    db0.tags(MemoTestClass(123)).add("tag-1")
    db0.commit()
    assert len(db0.find(MemoTestClass)) == 1
    assert len(db0.find(MemoTestClass, "tag-1")) == 1
    assert len(db0.find("tag-1")) == 1
    

def test_persisted_and_then_deleted_object_cannot_be_looked_up_by_type(db0_fixture):
    obj_1 = MemoTestClass(MemoTestClassWithMethods(0))
    db0.commit()
    db0.delete(obj_1)
    assert len(db0.find(MemoTestClassWithMethods)) == 0
    assert len(db0.find(MemoTestClass)) == 0


def test_automatic_type_tags_opt_out(db0_fixture):
    obj_1 = MemoNoDefTags(123)
    # object not found by type since it opted out of automatic tags
    assert len(db0.find(MemoNoDefTags)) == 0


def test_lookp_by_type_when_no_default_tags(db0_fixture):
    obj_1 = MemoNoDefTags(123)
    # NOTE: after adding first tag, we can look-up by type even when type opted out of automatic tags
    db0.tags(obj_1).add("tag-1")
    # object not found by type since it opted out of automatic tags
    assert len(db0.find(MemoNoDefTags)) == 1


def test_unable_to_apply_type_as_tag_directly(db0_fixture):
    obj_1 = MemoNoDefTags(123)
    # NOTE: types are auto-assigned and cannot be assigned as tags directly
    with pytest.raises(Exception):
        db0.tags(obj_1).add(MemoTestClass)


def test_apply_class_as_tag(db0_fixture):
    obj_1 = MemoNoDefTags(123)
    # NOTE: class can be appied as tag with a wrapper
    db0.tags(obj_1).add(db0.as_tag(MemoTestClass))
    assert len(db0.find(MemoTestClass)) == 0
    assert len(db0.find(db0.as_tag(MemoTestClass))) == 1
