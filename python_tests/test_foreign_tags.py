# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass, MemoTestSingleton
from .conftest import DB0_DIR


def test_foreign_object_assigned_as_tag(db0_fixture):
    px_name = db0.get_current_prefix().name
    foreign_uuid = db0.uuid(MemoTestSingleton(123))
    db0.close()
    db0.init(DB0_DIR)
    db0.open(px_name, "r")
    foreign_obj = db0.fetch(foreign_uuid)
    db0.open("some-other-prefix", "rw")
    # create object on a different prefix (new default)
    obj = MemoTestClass(456)
    assert db0.get_prefix_of(obj) != db0.get_prefix_of(foreign_obj)
    # try tagging with an object from the other prefix (i.e. foreign object)
    db0.tags(obj).add(db0.as_tag(foreign_obj))

    
def test_find_by_foreign_tag(db0_fixture):
    px_name = db0.get_current_prefix().name
    foreign_obj = MemoTestSingleton(123)
    db0.open("some-other-prefix", "rw")
    # create object on a different prefix (new default)
    obj = MemoTestClass(456)
    assert db0.get_prefix_of(obj) != db0.get_prefix_of(foreign_obj)
    # try tagging with an object from the other prefix (i.e. foreign object)
    db0.tags(obj).add(db0.as_tag(foreign_obj))

    # find on secondary prefix (default)
    assert len(db0.find(db0.as_tag(foreign_obj))) == 1
    assert next(iter(db0.find(db0.as_tag(foreign_obj)))) == obj
    
