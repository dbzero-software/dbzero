# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass


def test_two_identical_queries_have_identical_signatures(db0_fixture, memo_tags):
    q1 = db0.find("tag1")
    q2 = db0.find("tag1")
    assert q1.signature() == q2.signature()


def test_two_different_queries_have_different_signatures(db0_fixture, memo_tags):
    q1 = db0.find("tag1")
    q2 = db0.find("tag2")
    assert q1.signature() != q2.signature()


def test_similar_or_queries_have_identical_signatures(db0_fixture, memo_tags):
    q1 = db0.find(["tag1", "tag2", "tag3"])
    q2 = db0.find(["tag1", "tag2"])
    q3 = db0.find(["tag1", "tag3"])
    q4 = db0.find(["tag2", "tag3"])    
    assert q1.signature() in set([q2.signature(), q3.signature(), q4.signature()])    


def test_query_signatures_are_order_invariant(db0_fixture, memo_tags):
    q1 = db0.find(["tag1", "tag2", "tag3"])
    assert q1.signature() == db0.find(["tag3", "tag2", "tag1"]).signature()
    q2 = db0.find("tag1", "tag2", "tag3")
    assert q2.signature() == db0.find("tag2", "tag3", "tag1").signature()
