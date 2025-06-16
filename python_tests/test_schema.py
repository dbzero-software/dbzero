import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass


@db0.memo
class MemoSchemaTestClass:
    def __init__(self, value):
        self.value = value

def test_get_schema_when_consistent_type(db0_fixture):
    _ = [MemoSchemaTestClass(123), MemoSchemaTestClass(456)]
    schema = db0.get_schema(MemoSchemaTestClass)    
    assert schema["value"]["primary_type"] is int
    
    
def test_get_schema_when_inconsistent_types(db0_fixture):
    _ = [MemoSchemaTestClass(123), MemoSchemaTestClass("abc"), MemoSchemaTestClass("def")]
    schema = db0.get_schema(MemoSchemaTestClass)
    assert schema["value"]["primary_type"] is str

    
def test_get_schema_when_null_values_present(db0_fixture):
    _ = [MemoSchemaTestClass(None), MemoSchemaTestClass(None), MemoSchemaTestClass(456)]
    schema = db0.get_schema(MemoSchemaTestClass)
    assert schema["value"]["primary_type"] is int


def test_schema_mutations_by_object_updates(db0_fixture):
    objs = [MemoSchemaTestClass(123), MemoSchemaTestClass("abc"), MemoSchemaTestClass("def")]
    objs[1].value = 456
    schema = db0.get_schema(MemoSchemaTestClass)
    assert schema["value"]["primary_type"] is int
    
    
def test_schema_mutations_by_object_drops(db0_fixture):
    obj = MemoSchemaTestClass(123)
    obj_1, obj_2 = MemoSchemaTestClass("abc"), MemoSchemaTestClass("def")
    assert db0.get_schema(MemoSchemaTestClass)["value"]["primary_type"] is str
    db0.delete(obj_1)
    del obj_1
    db0.delete(obj_2)
    del obj_2
    db0.clear_cache()
    db0.commit()
    assert db0.get_schema(MemoSchemaTestClass)["value"]["primary_type"] is int
    
    
def test_schema_extension_with_dynamic_members(db0_fixture):
    obj = MemoSchemaTestClass(123)
    obj.value_2 = "new_value"
    obj.value_3 = 456.12    
    assert db0.get_schema(MemoSchemaTestClass)["value_2"]["primary_type"] is str
    assert db0.get_schema(MemoSchemaTestClass)["value_3"]["primary_type"] is float
    

def test_schema_collection_types(db0_fixture):
    obj = MemoSchemaTestClass(123)
    obj.value_1 = []
    obj.value_2 = {"a": 1, "b": 2}
    obj.value_3 = (1, 2, 3)
    obj.value_4 = {1, 2, 3}
    assert db0.get_schema(MemoSchemaTestClass)["value_1"]["primary_type"] is list
    assert db0.get_schema(MemoSchemaTestClass)["value_2"]["primary_type"] is dict
    assert db0.get_schema(MemoSchemaTestClass)["value_3"]["primary_type"] is tuple
    assert db0.get_schema(MemoSchemaTestClass)["value_4"]["primary_type"] is set
    
    
def test_schema_memo_type(db0_fixture):
    _ = MemoSchemaTestClass(MemoTestClass(123))
    assert db0.get_schema(MemoSchemaTestClass)["value"]["primary_type"] is db0.MemoBase    
