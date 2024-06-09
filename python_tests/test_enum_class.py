import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


@db0.enum(values=["RED", "GREEN", "BLUE"])
class Color:
    pass


def test_enum_can_be_defined_by_class(db0_fixture):
    assert str(Color.RED) == "RED"
