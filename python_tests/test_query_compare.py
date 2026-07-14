# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass


def test_compare_two_identical_queries(db0_fixture, memo_tags):
    q1 = db0.find("tag1")
    q2 = db0.find("tag1")
    assert q1.compare(q2) == 0.0


def test_compare_two_different_queries(db0_fixture, memo_tags):
    q1 = db0.find("tag1")
    q2 = db0.find("tag2")
    assert q1.compare(q2) == 1.0


def test_compare_simple_and_complex_query(db0_fixture, memo_tags):
    q1 = db0.find("tag1")
    q2 = db0.find("tag1", "tag2")
    q3 = db0.find(["tag1", "tag2"])
    # different queries
    assert q1.compare(q2) == 1.0
    # similar, but not identical queries
    assert q1.compare(q3) < 1.0
