# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestPxClass


def test_referencing_foreign_object_raises_error(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    with pytest.raises(Exception):
        # exception due to attept to reference obj_1 from obj_2 (which is on another prefix)
        obj_2 = MemoTestPxClass(obj_1, prefix=px_2)


def test_referencing_foreign_object_with_weak_proxy(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    assert db0.getrefcount(obj_1) == 0
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)    
    assert obj_2 is not None
    # make sure ref-count is not incremented
    assert db0.getrefcount(obj_1) == 0


def test_unreferencing_object_by_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)    
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    assert obj_2.value.value == 123


def test_weak_ref_copy(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    # initialize member with weak ref (copy)
    obj_3 = MemoTestPxClass(db0.weak_proxy(obj_2.value), prefix=px_2)
    assert obj_3.value.value == 123


def test_accessing_expired_long_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)    
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    # weak-ref expires when the pointed object is deleted
    del obj_1    
    db0.commit()
    # exception due to expired weak-ref
    with pytest.raises(db0.ReferenceError):
        assert obj_2.value.value == 123


def test_uuid_of_expired_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    uuid_1 = db0.uuid(obj_1)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    # weak-ref expires when the pointed object is deleted
    del obj_1    
    db0.commit()
    # even though the weak-ref is expired, the uuid of the object should be still retrievable
    uuid_2 = db0.uuid(obj_2.value)
    # uuid of expired weak proxy should be same as the one of the original object
    assert uuid_1 == uuid_2


def test_tag_lookup_by_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    obj_3 = MemoTestPxClass(456, prefix=px_2)
    # tag obj_3 using weak-tag - i.e. tag object from a foreign prefix
    db0.tags(obj_3).add(db0.as_tag(obj_2.value))
    assert list(db0.find(db0.as_tag(obj_2.value))) == [obj_3]
    # should work the same when using obj_1 as tag
    assert list(db0.find(db0.as_tag(obj_1))) == [obj_3]


def test_expired_function(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)    
    del obj_1
    db0.commit()
    assert db0.expired(obj_2.value) is True


def test_tag_lookup_by_expired_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    obj_3 = MemoTestPxClass(456, prefix=px_2)
    # assign non-expired, look-up by expired weak-ref
    db0.tags(obj_3).add(db0.as_tag(obj_2.value))
    del obj_1    
    db0.commit()
    assert list(db0.find(db0.as_tag(obj_2.value))) == [obj_3]


def test_tag_assign_by_expired_weak_ref(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1), prefix=px_2)
    obj_3 = MemoTestPxClass(456, prefix=px_2)
    del obj_1    
    db0.commit()
    # assign + find by expired weak-ref
    db0.tags(obj_3).add(db0.as_tag(obj_2.value))
    assert list(db0.find(db0.as_tag(obj_2.value))) == [obj_3]


def test_create_short_weak_ref(db0_fixture):    
    obj_1 = MemoTestPxClass(123)
    cnt_1 = db0.getrefcount(obj_1)
    # weak references can be also created to objects from the same prefix (i.e. short weak-ref)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1))
    # make sure weak-ref does not increment the ref-count
    assert db0.getrefcount(obj_1) == cnt_1


def test_unreference_by_short_weak_ref(db0_fixture):    
    obj_1 = MemoTestPxClass(123)
    cnt_1 = db0.getrefcount(obj_1)
    # weak references can be also created to objects from the same prefix (i.e. short weak-ref)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1))
    assert obj_2.value.value == 123
    assert not db0.expired(obj_2.value)
    
    
def test_tag_by_object_query_by_expired_weak_ref(db0_fixture):
    obj_1 = MemoTestPxClass(123)
    obj_2 = MemoTestPxClass(db0.weak_proxy(obj_1))
    obj_3 = MemoTestPxClass(999)
    db0.tags(obj_3).add(db0.as_tag(obj_1))
    del obj_1    
    db0.commit()
    # query by an expired weak-ref
    assert db0.expired(obj_2.value)
    assert list(db0.find(db0.as_tag(obj_2.value))) == [obj_3]


def test_long_weak_ref_inside_list(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")

    obj_1 = MemoTestPxClass(123, prefix=px_1)
    list_1 = db0.list([db0.weak_proxy(obj_1)])        
    assert list_1[0] == obj_1


def test_long_weak_ref_inside_tuple(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")
    
    obj_1 = MemoTestPxClass(123, prefix=px_1)
    tuple_1 = db0.tuple([db0.weak_proxy(obj_1)])        
    assert tuple_1[0] == obj_1


def test_long_weak_ref_inside_set(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")

    obj_1 = MemoTestPxClass(123, prefix=px_1)
    set_1 = db0.set([db0.weak_proxy(obj_1)])
    for obj in set_1:
        assert obj == obj_1    


def test_long_weak_ref_inside_dict(db0_fixture):
    px_1 = db0.get_current_prefix().name
    px_2 = "some-other-prefix"
    db0.open(px_2, "rw")

    obj_1 = MemoTestPxClass(123, prefix=px_1)
    dict_1 = db0.dict({"key": db0.weak_proxy(obj_1)})
    for _, value in dict_1.items():
        assert value == obj_1
