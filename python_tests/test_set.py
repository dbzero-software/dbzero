import pytest
import datetime
import random
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


@db0.memo
class CollisionClass:
    def __init__(self, value):
        self.value = int(value)

    def __eq__(self, other):
        return self.value == other.value

    def __hash__(self):
        return self.value % 10


def make_python_set(values = None):
    if values:
        return set(values)
    return set()

def make_db0_set(values = None):
    if values:
        return db0.set(values)
    return db0.set()

set_test_params = [(make_python_set), (make_db0_set)]

def test_db0_set_can_be_created(db0_fixture):
    set_1 = db0.set()    
    assert set_1 is not None    


def test_can_create_set_with_params(db0_fixture):
    set1 = db0.set([1,2,3,4,5,1,2])
    assert len(set1) == 5

def test_can_compare_db0_set_to_python_set(db0_fixture):
    set1 = db0.set([1,2,3,4,5,1,2])
    assert set1 == {1,2,3,4,5}
    assert set1 != {1,2,3,4,5,6}

def test_can_compare_sets(db0_fixture):
    set1 = db0.set([1,2,3,4,5,1,2])
    set2 = db0.set([1,2,3,4,5])
    set3 = db0.set([1,2,3,4])
    assert set1 == set2
    assert not set1 == set3
    assert not set1 != set2
    assert set1 != set3


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_is_initially_empty(db0_fixture, make_set):
    set_1 = make_set()
    assert len(set_1) == 0


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_add_elements(db0_fixture, make_set):
    set_1 = make_set()
    assert len(set_1) == 0
    set_1.add(1)
    set_1.add(2)
    set_1.add(3)
    set_1.add(1)
    assert len(set_1) == 3


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_add_elements_with_different_types(db0_fixture, make_set):
    set_1 = make_set()
    assert len(set_1) == 0
    set_1.add(1)
    set_1.add("2")
    set_1.add(3.2)
    set_1.add("SomeString")
    set_1.add(1)
    set_1.add("SomeString")
    assert len(set_1) == 4

@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_iterate(db0_fixture, make_set):
    set_1 = make_set()
    assert len(set_1) == 0
    elems = [1,"2", "SomeString", 3.2]
    set_1.add(1)
    set_1.add("2")
    set_1.add(3.2)
    set_1.add("SomeString")
    set_1.add(1)
    set_1.add("SomeString")
    assert len(set_1) == 4
    for elem in set_1:
        assert elem in elems


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_use_in(db0_fixture, make_set):
    set_1 = make_set()
    assert len(set_1) == 0
    elems = [1,"2", "SomeString", 3.2]
    set_1.add(1)
    set_1.add("2")
    set_1.add(3.2)
    set_1.add("SomeString")
    set_1.add(1)
    set_1.add("SomeString")
    assert len(set_1) == 4
    for elem in elems:
        assert elem in set_1

    assert 52 not in set_1

@pytest.mark.parametrize("make_set", set_test_params)
def test_set_isdisjoint(db0_fixture, make_set):
    set_1 = make_set([1,2,3,4])
    set_2 = make_set([5,6,7,8])
    set_3 = make_set([3,4,5,6])
    assert set_1.isdisjoint(set_2)
    assert not set_1.isdisjoint(set_3)
    assert set_1.isdisjoint([5,6,7,8])
    assert not set_1.isdisjoint([3,4,5,6])

@pytest.mark.parametrize("make_set", set_test_params)
def test_set_issubset(db0_fixture, make_set):
    set_1 = make_set([1,3,2])
    set_2 = make_set([2,3,1,2,4])
    set_3 = make_set([2,3,5,6,7])
    assert set_1.issubset(set_2)
    assert not set_1.issubset(set_3)
    assert set_1.issubset(set_1)
    assert set_1.issubset([2,3,1,2,4])
    assert not set_1.issubset([2,3,5,6,7])

@pytest.mark.parametrize("make_set", set_test_params)
def test_set_issubset_le(db0_fixture, make_set):
    set_1 = make_set([1,3,2])
    set_2 = make_set([2,3,1,2,4])
    set_3 = make_set([2,3,5,6,7])
    assert set_1 <= set_2
    assert not set_1 <= set_3
    assert set_1 <= set_1
    assert set_1 <= set([2,3,1,2,4])
    assert not set_1 <= set([2,3,5,6,7])

@pytest.mark.parametrize("make_set", set_test_params)
def test_set_issubset_lt(db0_fixture, make_set):
    set_1 = make_set([1,3,2])
    set_2 = make_set([2,3,1,2,4])
    set_3 = make_set([2,3,5,6,7])
    set_4 = make_set([1,2,5])
    assert set_1 < set_2
    assert not set_1 < set_3
    assert not set_1 < set_1
    assert not set_1 < set_4
    assert set_1 < set([2,3,1,2,4])
    assert not set_1 < set([2,3,5,6,7])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_issuperset(db0_fixture, make_set):
    set_1 = make_set([1,2,3,4,5,6])
    set_2 = make_set([2,3,1,2,4])
    set_3 = make_set([2,3,5,6,7])
    assert set_1.issuperset(set_2)
    assert not set_1.issuperset(set_3)
    assert set_1.issuperset(set_1)
    assert set_1.issuperset([2,3,1,2,4])
    assert not set_1.issuperset([2,3,5,6,7])

@pytest.mark.parametrize("make_set", set_test_params)
def test_set_issuperset_ge(db0_fixture, make_set):
    set_1 = make_set([1,2,3,4,5,6])
    set_2 = make_set([2,3,1,2,4])
    set_3 = make_set([2,3,5,6,7])
    assert set_1 >= set_2
    assert not set_1 >= set_3
    assert set_1 >= set_1
    assert set_1 >= set([2,3,1,2,4])
    assert not set_1 >= set([2,3,5,6,7])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_issubset_gt(db0_fixture, make_set):
    set_1 = make_set([1,2,3,4,5,6])
    set_2 = make_set([2,3,1,2,4])
    set_3 = make_set([2,3,5,6,7])
    set_4 = make_set([1,2,3,4,5,7])
    assert set_1 > set_2
    assert not set_1 > set_3
    assert not set_1 > set_1
    assert not set_1 > set_4

    assert set_1 > set([2,3,1,2,4])
    assert not set_1 > set([2,3,5,6,7])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_copy(db0_fixture, make_set):
    set_1 = make_set([1,3,2])
    set_copy = set_1.copy()
    assert set_copy == set([1,3,2])
    set_copy.add(7)
    assert len(set_copy) == 4
    assert set_copy != set([1,3,2])


def test_set_union_issue(db0_fixture):
    set_1 = db0.set([1])
    set_2 = db0.set([3])
    set_union = set_1.union(set_2)
    assert set(set_union) == set([1, 3])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_union(db0_fixture, make_set):
    set_1 = db0.set([1, 3, 2])    
    set_2 = db0.set([3, 4, 5])    
    set_3 = db0.set([5, 6, 7])
    set_union = set_1.union(set_2, set_3)    
    assert set(set_union) == set([1, 2, 3, 4, 5, 6, 7])


def test_set_can_union_db0_set_with_python_sets(db0_fixture):
    set_1 = db0.set([1, 3, 2])
    set_2 = set([3, 4, 5])
    set_3 = set([5, 6, 7])
    set_union = set_1.union(set_2, set_3)
    assert set_union == set([1, 2, 3, 4, 5, 6, 7])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_union_as_or(db0_fixture, make_set):
    set_1 = make_set([1, 3, 2])
    set_2 = make_set([3, 4, 5])
    set_3 = make_set([5, 6, 7])
    set_union = set_1 | set_2 | set_3
    assert set_union == set([1, 2, 3, 4, 5, 6, 7])
    assert len(set_1) == 3
    assert len(set_2) == 3
    assert len(set_3) == 3


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_intersection(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4, 5, 6, 7, 8])
    set_2 = make_set([3, 4, 5])
    set_3 = make_set([5, 6, 7])
    set_union = set_1.intersection(set_2, set_3)
    assert set_union == set([5])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_intersection_as_op(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4, 5, 6, 7, 8])
    set_2 = make_set([3, 4, 5])
    set_3 = make_set([5, 6, 7])
    set_union = set_1 & set_2 & set_3
    assert set_union == set([5])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_difference(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4, 5, 6, 7, 8, 9])
    set_2 = make_set([3, 4, 5])
    set_3 = make_set([6, 7, 8, 10 , 11])
    set_union = set_1.difference(set_2, set_3)
    assert len(set_union) == 3
    assert set_union == set([1, 2, 9])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_difference_as_operator(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4, 5, 6, 7, 8, 9])
    set_2 = make_set([3, 4, 5])
    set_3 = make_set([6, 7, 8, 10 , 11])
    set_union = set_1 - set_2 - set_3
    assert len(set_union) == 3
    assert set_union == set([1, 2, 9])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_symmetric_difference(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4, 5, 6, 7, 8, 9])
    set_2 = make_set([3, 4, 5, 6, 7, 8, 10, 11])
    set_union = set_1.symmetric_difference(set_2)
    assert len(set_union) == 5
    assert set_union == set([1, 2, 9, 10, 11])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_difference_as_operator(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4, 5, 6, 7, 8, 9])
    set_2 = make_set([3, 4, 5, 6, 7, 8, 10, 11])
    set_union = set_1 ^ set_2
    assert len(set_union) == 5
    assert set_union == set([1, 2, 9, 10, 11])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_remove_item(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4])
    set_1.remove(2)
    assert len(set_1) == 3
    assert set_1 == set([1, 3, 4])

    set_2 = make_set(["a", "b", "c", "d"])
    set_2.remove("b")
    assert len(set_2) == 3
    assert set_2 == set(["a", "c", "d"])
    
    with pytest.raises(KeyError):
        set_1.remove(10)
    
    with pytest.raises(KeyError):
        set_2.remove("g")


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_discard_item(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4])
    set_1.discard(2)
    assert len(set_1) == 3
    assert set_1 == set([1, 3, 4])

    set_2 = make_set(["a", "b", "c", "d"])
    set_2.discard("b")
    assert len(set_2) == 3
    assert set_2 == set(["a", "c", "d"])
    set_1.discard(10)
    set_2.discard("g")


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_pop(db0_fixture, make_set):
    items = [1, 2, 3, 4]
    set_1 = make_set(items)
    item = set_1.pop()
    assert len(set_1) == 3
    assert item in items
    item = set_1.pop()
    assert len(set_1) == 2
    assert item in items
    item = set_1.pop()
    assert len(set_1) == 1
    assert item in items
    item = set_1.pop()
    assert len(set_1) == 0
    assert item in items
    with pytest.raises(KeyError):
        item = set_1.pop()


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_clear_items(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4])
    set_1.clear()
    assert len(set_1) == 0


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_can_intersect_in_place(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4, 5])
    set_2 = make_set([3, 4, 5, 6])
    set_1 &= set_2
    assert set_1 == set([3, 4, 5])
    set_1 &= set([5, 6, 7, 8])
    assert set_1 == set([5])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_difference_in_place(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4, 5])
    set_2 = make_set([3, 4, 7])
    set_1 -= set_2
    assert set_1 == set([1, 2, 5])
    set_1 -= set([2, 5, 9])
    assert set_1 == set([1])


@pytest.mark.parametrize("make_set", set_test_params)
def test_set_symmetric_difference_in_place(db0_fixture, make_set):
    set_1 = make_set([1, 2, 3, 4, 5])
    set_2 = make_set([3, 4, 7])
    set_1 ^= set_2
    assert len(set_1) == 4
    assert set_1 == set([1, 2, 5, 7])
    set_1 ^= set([2, 5, 7, 9])
    assert set_1 == set([1, 9])


# FIXME: test crashing with segfault
# def test_set_items_in(db0_fixture):
#     # tests iteration over values from set
#     set_1 = db0.set()
#     # insert 1000 random items
#     for i in range(100):
#         set_1.add(i)
#     assert len(set_1) == 100
#     now = datetime.datetime.now()
#     for i in range(1000000):
#         random_int = random.randint(0, 300)
#         if random_int < 100:
#             assert random_int in set_1
#         else:
#             assert random_int not in set_1
#     end = datetime.datetime.now()
#     print("Elapsed time: ", end - now)


def test_set_as_member(db0_fixture):
    object_1 = MemoTestClass(set([1,2,3,4,5]))
    assert set(object_1.value) == set([1,2,3,4,5])
    

def test_set_update_after_commit(db0_fixture):
    prefix_name = db0.get_current_prefix()
    object_1 = MemoTestClass(set([1,2,3,4,5]))
    root = MemoTestSingleton(object_1)
    uuid = db0.uuid(object_1)
    del object_1
    db0.commit()
    db0.close()
    db0.init(DB0_DIR)
    db0.open(prefix_name)
    object_2 = db0.fetch(uuid)
    object_2.value.add(6)
    assert set(object_2.value) == set([1,2,3,4,5,6])

    
# fixme: Needs to fix problem with __eq__ method
# @pytest.mark.parametrize("make_set", set_test_params)
# def test_set_items_collisions(db0_fixture, make_set):
#     # tests iteration over values from set
#     set_1 = make_set()
#     set_1.add(CollisionClass(1))
#     set_1.add(CollisionClass(2))
#     set_1.add(CollisionClass(3))
#     set_1.add(CollisionClass(14))
#     set_1.add(CollisionClass(22))
#     set_1.add(CollisionClass(33))
#     assert(len(set_1) == 6)
#     assert CollisionClass(1) in set_1
#     assert CollisionClass(2) in set_1
#     assert CollisionClass(3) in set_1
#     assert CollisionClass(14) in set_1
#     assert CollisionClass(22) in set_1
#     assert CollisionClass(33) in set_1
#     assert CollisionClass(4) not in set_1
    