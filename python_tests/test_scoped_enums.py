import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


@db0.memo(prefix="scoped-class-prefix")
class ScopedDataClass:
    def __init__(self, value):
        self.value = value

    
@db0.enum(values=["RED", "GREEN", "BLACK"], prefix="scoped-class-prefix")
class ScopedColor:
    pass    


@db0.enum(values=["RED", "GREEN", "BLACK"], prefix=None)
class XColor:
    pass

    
def test_enum_with_null_prefix_is_no_scoped(db0_fixture):
    obj = MemoTestClass(42)
    db0.tags(obj).add(XColor.RED)
    assert len(list(db0.find(MemoTestClass, XColor.RED))) == 1


def test_scoped_class_can_be_tagged_with_scoped_enum(db0_fixture):
    obj = ScopedDataClass(42)
    db0.tags(obj).add(ScopedColor.RED)
    assert len(list(db0.find(ScopedDataClass, ScopedColor.RED))) == 1
