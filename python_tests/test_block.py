import pytest
import dbzero_ce as db0


def test_db0_block_can_be_created(db0_fixture):
    block_1 = db0.block()    
    assert block_1 is not None    


def test_db0_block_is_initially_empty(db0_fixture):
    block_1 = db0.block()
    assert len(block_1) == 0

def test_db0_block_append(db0_fixture):
    block_1 = db0.block()
    assert len(block_1) == 0
    block_1.append("asd")
    block_1.append("qwe")
    assert len(block_1) == 2

def test_db0_block_get_item(db0_fixture):
    block_1 = db0.block()
    assert len(block_1) == 0
    block_1.append("asd")
    block_1.append("qwe")
    assert len(block_1) == 2
    assert block_1[0] == "asd"
    assert block_1[1] == "qwe"


def test_db0_block_in_list(db0_fixture):
    block_1 = db0.block()
    assert len(block_1) == 0
    block_1.append("asd")
    block_1.append("qwe")
    assert len(block_1) == 2
    assert block_1[0] == "asd"
    assert block_1[1] == "qwe"
    list = db0.list()
    list.append(block_1)