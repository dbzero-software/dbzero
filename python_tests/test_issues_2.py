# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from datetime import datetime
from .memo_test_types import MemoTestClass


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


def test_multiple_commits_rollback_object_issue1(db0_fixture):
    obj = Attribute("1", "1")
    assert obj.name == "1"
    assert obj.value == "1"
    obj.tag_object(["object"])
    db0.commit()
    obj1 = [x for x in db0.find(Attribute, "object")][0]
    obj1.name = "2"
    obj1.value = "2"
    db0.commit()
    assert obj1.name == "2"
    assert obj1.value == "2"
    obj1.tag_object(["object2"])
    db0.commit()
    db0.commit()
    assert obj1.name == "2"
    assert obj1.value == "2"


def test_tagging_and_untagging_in_single_commit_breaks_tag_search(db0_fixture):
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
