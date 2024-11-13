import pytest
import dbzero_ce as db0
from .memo_test_types import KVTestClass, MemoTestClass, MemoDataPxClass
from .conftest import DB0_DIR, DATA_PX


def test_simple_group_by_query(db0_fixture):
    keys = ["one", "two", "three"]
    objects = []
    for i in range(10):
        objects.append(KVTestClass(keys[i % 3], i))
    
    db0.tags(*objects).add("tag1")
    # commit in order to make results available in snapshot
    db0.commit()
    groups = db0.group_by(lambda row: row.key, db0.find("tag1"))
    assert len(groups) == 3
    assert groups["one"].count() == 4
    assert groups["two"].count() == 3
    assert groups["three"].count() == 3
    

# FIXME: failing test blocked
# def test_delta_group_by_query(db0_fixture):
#     keys = ["one", "two", "three"]
#     objects = []
#     for i in range(10):
#         objects.append(KVTestClass(keys[i % 3], i))
    
#     db0.tags(*objects).add("tag1")
#     db0.commit()
#     # first group by to feed the internal cache
#     # we pass max_scan = 1 to force the internal cache to be populated
#     db0.group_by(lambda row: row.key, db0.find("tag1"), max_scan = 1)
#     db0.commit()
    
#     # assign tags to 2 more objects
#     db0.tags(KVTestClass("one", 11)).add("tag1")
#     db0.tags(KVTestClass("three", 12)).add("tag1")
#     db0.commit()
    
#     # run as delta query
#     groups = db0.group_by(lambda row: row.key, db0.find("tag1"))
#     assert len(groups) == 3
#     assert groups["one"].count() == 5
#     assert groups["two"].count() == 3
#     assert groups["three"].count() == 4


# FIXME: failing test blocked
# def test_delta_query_with_removals(db0_fixture):
#     keys = ["one", "two", "three"]
#     objects = []
#     for i in range(10):
#         objects.append(KVTestClass(keys[i % 3], i))
    
#     db0.tags(*objects).add("tag1")
#     db0.commit()
#     # first group by to feed the internal cache
#     db0.group_by(lambda row: row.key, db0.find("tag1"), max_scan = 1)
    
#     # remove tags from 2 objects
#     db0.tags(objects[1], objects[6]).remove("tag1")
#     db0.commit()
    
#     # run as delta query
#     groups = db0.group_by(lambda row: row.key, db0.find("tag1"), max_scan = 1)
#     print(dict(groups))
#     assert len(groups) == 3
#     assert groups["one"].count() == 3
#     assert groups["two"].count() == 2
#     assert groups["three"].count() == 3


# FIXME: failing test blocked
# def test_delta_from_non_identical_queries(db0_fixture):
#     keys = ["one", "two", "three"]
#     objects = []
#     for i in range(10):
#         objects.append(KVTestClass(keys[i % 3], i))
    
#     db0.tags(*objects).add("tag1")
#     db0.tags(*objects).add("tag2")
#     db0.tags(*objects).add("tag3")
#     db0.commit()
    
#     # first group by to feed the internal cache
#     db0.group_by(lambda row: row.key, db0.find(["tag1", "tag2", "tag3"]), max_scan = 1)
    
#     # assign tag3 to 2 more objects
#     db0.tags(KVTestClass("one", 11)).add("tag4")
#     db0.tags(KVTestClass("three", 12)).add("tag4")
#     db0.commit()
    
#     # run as delta query (but adding the additional optional tag)
#     groups = db0.group_by(lambda row: row.key, db0.find(["tag1", "tag2", "tag3", "tag4"]), max_scan = 1)
#     assert len(groups) == 3
#     assert groups["one"].count() == 5
#     assert groups["two"].count() == 3
#     assert groups["three"].count() == 4


def test_group_by_enum_values(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    # commit to make results available in snapshot
    db0.commit()
    groups = db0.group_by(Colors.values(), db0.find(MemoTestClass))
    assert len(groups) == 3
    assert groups[Colors.RED].count() == 4
    assert groups[Colors.GREEN].count() == 3
    assert groups[Colors.BLUE].count() == 3


# FIXME: failing test blocked
# def test_group_by_enum_values_with_tag_removals(db0_fixture, memo_enum_tags):
#     # use max_scan = 1 to force the internal cache to be updated
#     Colors = memo_enum_tags["Colors"]
#     db0.commit()    
#     assert db0.group_by(Colors.values(), db0.find(MemoTestClass), max_scan = 1)[Colors.RED].count() == 4    
#     i = 0
#     for _ in range(4):
#         db0.tags(next(db0.find(MemoTestClass, Colors.RED))).remove(Colors.RED)
#         i += 1
#         db0.commit()
#         result = db0.group_by(Colors.values(), db0.find(MemoTestClass), max_scan = 1).get(Colors.RED, None)        
#         count = result.count() if result else 0
#         assert count == 4 - i
    
    
def test_group_by_multiple_criteria(db0_fixture, memo_enum_tags):
    Colors = memo_enum_tags["Colors"]
    db0.commit()
    # group by all colors and then by even/odd values
    groups = db0.group_by((Colors.values(), lambda x: x.value % 2), db0.find(MemoTestClass))
    assert len(groups) == 6
    assert groups[(Colors.RED, 0)].count() == 2
    assert groups[(Colors.RED, 1)].count() == 2
    
    
def test_fast_query_with_separate_prefix_for_cache(db0_fixture, memo_scoped_enum_tags):
    db0.close()
    db0.init(DB0_DIR)
    db0.open(DATA_PX, "r")
    fq_prefix = "px-fast-query"
    db0.open(fq_prefix, "rw")
    db0.init_fast_query(fq_prefix)
    Colors = memo_scoped_enum_tags["Colors"]
    # first run to feed cache
    db0.group_by((Colors.values(), lambda x: "test"), db0.find(MemoDataPxClass), max_scan=1)    
    db0.close()
    
    db0.init(DB0_DIR)
    db0.open(DATA_PX, "r")
    db0.open(fq_prefix, "rw")
    
    # run again to use cache
    groups = db0.group_by((Colors.values(), lambda x: "test"), db0.find(MemoDataPxClass), max_scan=1)
    # make sure result retured from cache
    assert type(groups) == type(db0.dict())    
