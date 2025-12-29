# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from random import random
import pytest
import dbzero as db0
from dataclasses import dataclass
from .conftest import DB0_DIR
import random


@db0.memo(immutable=True, no_default_tags=True)
@dataclass
class MemoImmutableClass1:
    data: str
    value: int = 0
    
def test_create_memo_immutable(db0_fixture):
    _ = MemoImmutableClass1(data="immutable data", value=42)


def test_tag_and_find_immutable_instance(db0_fixture):
    obj_1 = MemoImmutableClass1(data="immutable data", value=42)
    db0.tags(obj_1).add("tag1", "tag2")
    assert list(db0.find("tag1")) == [obj_1]
    