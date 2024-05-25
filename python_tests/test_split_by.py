import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


def test_query_can_be_split_by_list_of_tags(db0_fixture, memo_tags):
    query = db0.split_by(["tag1", "tag2", "tag3", "tag4"], db0.find(MemoTestClass))
    assert len(list(query)) == 10
