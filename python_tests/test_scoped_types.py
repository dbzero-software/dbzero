import pytest
import dbzero_ce as db0


@db0.memo(prefix="scoped-class-prefix")
class DataClass:
    def __init__(self, value):
        self.value = value
    
    
@db0.enum(values=["RED", "GREEN", "BLACK"], prefix="scoped-class-prefix")
class ScopedColor:
    pass


def test_get_type_info_from_scoped_class(db0_fixture):
    type_info = db0.get_type_info(DataClass)    
    assert type_info["prefix"] == "scoped-class-prefix"
    
    
def test_create_scoped_class_instance(db0_fixture):
    obj = DataClass(42)
    assert db0.get_prefix(obj) != db0.get_current_prefix()
    
    
def test_scoped_class_can_be_tagged_with_scoped_enum(db0_fixture):
    obj = DataClass(42)
    db0.tags(obj).add(ScopedColor.RED)
    assert len(list(db0.find(DataClass, ScopedColor.RED))) == 1
    