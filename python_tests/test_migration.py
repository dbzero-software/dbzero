# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

import pytest
import dbzero as db0
import datetime
import subprocess
import sys


@pytest.mark.parametrize("depth", [2, 3])
def test_indexed_field_migration_uses_descendant_field_names(tmp_path, depth):
    code = r'''
import dbzero as db0
import sys

root, phase, depth = sys.argv[1:]
db0.init(root, autocommit=False, async_autocommit=False, file_prealloc=False)
db0.open("migration", "rw", autocommit=False)

@db0.memo(id="layout-migration-base")
class Base:
    def __init__(self, value, rank, assign=True):
        if assign:
            self.value = value
            self.rank = rank

@db0.memo(id="layout-migration-middle")
class Middle(Base):
    def __init__(self, value, rank, assign=True):
        self.middle_decoy = 777
        super().__init__(value, rank, assign)

@db0.memo(id="layout-migration-derived")
class Derived(Middle if depth == "3" else Base):
    def __init__(self, value, rank, assign=True):
        self.unrelated = 999
        self.other = 888
        super().__init__(value, rank, assign)

if phase == "write":
    for name, obj in (
        ("base", Base(11, 101)),
        ("child", Derived(22, 202)),
        ("null", Derived(None, None)),
        ("absent", Derived(None, None, False)),
    ):
        db0.tags(obj).add(name)
else:
    db0.indexed_fields("value", "rank")(Base)
    objects = {name: next(iter(db0.find(name))) for name in ("base", "child", "null", "absent")}
    child_value = 33 if phase == "read" else 22
    child_rank = 303 if phase == "read" else 202
    for field, base_value, value, decoy in (
        ("value", 11, child_value, 999),
        ("rank", 101, child_rank, 888),
    ):
        index = db0.index_of(Base, field)
        def matches(key):
            return {db0.uuid(obj) for obj in db0.find(Base, index.select(key, key))}
        assert matches(base_value) == {db0.uuid(objects["base"])}
        assert matches(value) == {db0.uuid(objects["child"])}, (field, value, matches(value))
        assert matches(decoy) == set()
        if depth == "3":
            middle_index = db0.index_of(Middle, field)
            assert list(db0.find(Middle, middle_index.select(value, value))) == [objects["child"]]
            assert list(db0.find(Middle, middle_index.select(base_value, base_value))) == []
            assert list(db0.find(Middle, middle_index.select(777, 777))) == []
        assert len(index) == 3
        assert {db0.uuid(obj) for obj in index.sort(db0.find(Base), null_first=True)} == {
            db0.uuid(objects[name]) for name in ("base", "child", "null")
        }
    if phase == "migrate":
        objects["child"].value = 33
        objects["child"].rank = 303
        query_type = Middle if depth == "3" else Base
        for field, old, new in (("value", 22, 33), ("rank", 202, 303)):
            index = db0.index_of(query_type, field)
            assert list(db0.find(query_type, index.select(old, old))) == []
            assert list(db0.find(query_type, index.select(new, new))) == [objects["child"]]
            delattr(objects["child"], field)
            assert list(db0.find(query_type, index.select(new, new))) == []
            setattr(objects["child"], field, new)
            assert list(db0.find(query_type, index.select(new, new))) == [objects["child"]]
db0.commit()
db0.close()
'''
    for phase in ("write", "migrate", "read"):
        result = subprocess.run(
            [sys.executable, "-c", code, str(tmp_path), phase, str(depth)],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"{phase}: {result.stdout}{result.stderr}"


def test_tag_field_migration_uses_descendant_field_names(tmp_path):
    code = r'''
import dbzero as db0
import sys

root, phase = sys.argv[1:]
db0.init(root, autocommit=False, async_autocommit=False, file_prealloc=False)
db0.open("migration", "rw", autocommit=False)

# Add declarations only after objects with different layouts are persisted.
fields = ()
if phase != "write":
    fields = ("value", "rank")

@db0.memo(id="tag-layout-migration-base")
@db0.tag_fields(*fields)
class Base:
    def __init__(self, value, rank, assign=True):
        if assign:
            self.value = value
            self.rank = rank

@db0.memo(id="tag-layout-migration-derived")
class Derived(Base):
    def __init__(self, value, rank, assign=True):
        # Base field IDs now identify unrelated fields in this layout.
        self.unrelated = "decoy-value"
        self.other = "decoy-rank"
        super().__init__(value, rank, assign)

if phase == "write":
    objects = {
        "base": Base("base-value", "base-rank"),
        "child": Derived("child-value", "child-rank"),
        "null": Derived(None, None),
        "absent": Derived(None, None, False),
    }
    for name, obj in objects.items():
        db0.tags(obj).add("record-" + name)
else:
    objects = {name: next(iter(db0.find("record-" + name)))
               for name in ("base", "child", "null", "absent")}

for name in ("base", "child"):
    expected = set() if phase == "write" else {db0.uuid(objects[name])}
    for tag in (name + "-value", name + "-rank"):
        assert {db0.uuid(obj) for obj in db0.find(Base, tag)} == expected
        if name == "child":
            assert {db0.uuid(obj) for obj in db0.find(Derived, tag)} == expected
for decoy in ("decoy-value", "decoy-rank"):
    assert list(db0.find(Base, decoy)) == []
assert {db0.uuid(obj) for obj in db0.find(Base)} == {db0.uuid(obj) for obj in objects.values()}
db0.commit()
db0.close()
'''
    for phase in ("write", "migrate", "read"):
        result = subprocess.run(
            [sys.executable, "-c", code, str(tmp_path), phase],
            capture_output=True, text=True, timeout=30,
        )
        assert result.returncode == 0, f"{phase}: {result.stdout}{result.stderr}"


@db0.memo
class MigrationTestClass:
    def __init__(self):
        self.int_value = 931
        self.str_value = "abdc"
        self.date_value = datetime.date(2023, 1, 1)
        self.list_value = []
        self.dict_value = {}
        self.set_value = set()


@db0.memo
class MigrationTestClassBase:
    def __init__(self):
        self.int_value = 931
        self.str_value = "abdc"
        self.date_value = datetime.date(2023, 1, 1)
        self.list_value = []
        self.dict_value = {}
        self.set_value = set()

@db0.memo
class MigrationTestClassDerived(MigrationTestClassBase):
    def __init__(self):
        super().__init__()
        self.ext_int_value = 123
        self.ext_str_value = "abcd"
        self.ext_date_value = datetime.date(2023, 1, 1)


def test_change_field_name_inside_class(db0_fixture):
    def new_init(self):
        self.int_value = 122
        # NOTE: this field name is changed
        self.str_new_value = "defg"
        self.date_value = datetime.date(2025, 1, 1)
        self.list_value = []
        self.dict_value = {}
        self.set_value = set()

    # Create an instance of the class
    obj_1 = MigrationTestClass()
    assert obj_1.int_value == 931
    assert obj_1.str_value == "abdc"
    assert obj_1.date_value == datetime.date(2023, 1, 1)
    
    # migrate the class by changing the __init__ method
    MigrationTestClass.__init__ = new_init
    obj_2 = MigrationTestClass()
    assert obj_2.int_value == 122
    assert obj_2.str_new_value == "defg"
    assert obj_2.date_value == datetime.date(2025, 1, 1)
    
        
def test_change_field_name_inside_derived_class(db0_fixture):
    def new_init(self):
        super(self.__class__, self).__init__()
        self.ext_int_value = 122
        self. _ext_str_new_value = "defg"
        self.ext_date_value = datetime.date(2025, 1, 1)
    
    # Create an instance of the class
    obj_1 = MigrationTestClassDerived()
    assert obj_1.int_value == 931
    assert obj_1.str_value == "abdc"
    assert obj_1.ext_int_value == 123
    assert obj_1.ext_str_value == "abcd"
    
    # migrate the class by changing the __init__ method
    MigrationTestClassDerived.__init__ = new_init
    obj_2 = MigrationTestClassDerived()
    assert obj_2.ext_int_value == 122
    assert obj_2._ext_str_new_value == "defg"
    assert obj_2.ext_date_value == datetime.date(2025, 1, 1)
    