import pytest
import dbzero_ce as db0


@db0.memo(prefix="scoped-class-prefix")
class DataClass:
    def __init__(self, value):
        self.value = value
    
    
def test_get_type_info_from_scoped_class(db0_fixture):
    type_info = db0.get_type_info(DataClass)    
    assert type_info["prefix"] == "scoped-class-prefix"
    
    
def test_create_scoped_class_instance(db0_fixture):
    obj = DataClass(42)
    assert db0.get_prefix(obj) != db0.get_current_prefix()