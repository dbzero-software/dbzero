import pytest
import dbzero_ce as db0


@db0.memo
class MemoSchemaTestClass:
    def __init__(self, value):
        self.value = value

def test_get_schema_when_consistent_type(db0_fixture):
    _ = [MemoSchemaTestClass(123), MemoSchemaTestClass(456)]
    schema = db0.get_schema(MemoSchemaTestClass)    
    assert schema["value"]["primary_type"] == "int"    
    
    
def test_get_schema_when_inconsistent_types(db0_fixture):
    _ = [MemoSchemaTestClass(123), MemoSchemaTestClass("abc"), MemoSchemaTestClass("def")]
    schema = db0.get_schema(MemoSchemaTestClass)
    assert schema["value"]["primary_type"] == "str"

        
def test_get_schema_when_null_values_present(db0_fixture):
    _ = [MemoSchemaTestClass(None), MemoSchemaTestClass(None), MemoSchemaTestClass(456)]
    schema = db0.get_schema(MemoSchemaTestClass)    
    assert schema["value"]["primary_type"] == "int"


def test_schema_mutations_by_object_updates(db0_fixture):
    objs = [MemoSchemaTestClass(123), MemoSchemaTestClass("abc"), MemoSchemaTestClass("def")]
    objs[1].value = 456
    schema = db0.get_schema(MemoSchemaTestClass)
    assert schema["value"]["primary_type"] == "int"
    
    
def test_schema_mutations_by_object_drops(db0_fixture):
    obj = MemoSchemaTestClass(123)
    obj_1, obj_2 = MemoSchemaTestClass("abc"), MemoSchemaTestClass("def")
    assert db0.get_schema(MemoSchemaTestClass)["value"]["primary_type"] == "str"
    db0.delete(obj_1)
    del obj_1
    db0.delete(obj_2)
    del obj_2
    db0.clear_cache()
    db0.commit()
    assert db0.get_schema(MemoSchemaTestClass)["value"]["primary_type"] == "int"
    