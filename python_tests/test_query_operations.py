import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


def test_compare_two_identical_queries(db0_fixture, memo_tags):
    q1 = db0.find("tag1")
    q2 = db0.find("tag1")
    assert q1.compare(q2) == 0.0


def test_compare_two_different_queries(db0_fixture, memo_tags):
    q1 = db0.find("tag1")
    q2 = db0.find("tag2")
    assert q1.compare(q2) == 1.0
