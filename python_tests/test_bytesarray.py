import dbzero_ce as db0


def test_db0_bytearray_can_be_created(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')    
    assert bytearray_1 is not None    

def test_db0_bytearray_can_get_value_by_index(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')    
    assert bytearray_1[0] == 97