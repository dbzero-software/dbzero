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
    