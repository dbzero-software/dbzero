import pytest
import dbzero_ce as db0
from .memo_test_types import MemoTestClass, TriColor, MemoTestSingleton
from dataclasses import dataclass, field
from datetime import datetime


@db0.memo
@dataclass
class MemoDataClass:
    title: str
    author: str
    year: int = 2024
    is_available: bool = True
    
def formatted_name(prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}"

@db0.memo
@dataclass
class MemoDataClassGenArgs:
    event_type: str
    event_name: str = formatted_name("a", "b")


@db0.memo
@dataclass
class MemoDataClassArgsFactory:
    event_type: str
    event_name: datetime = field(default_factory=datetime.now)
    
    
def test_memo_dataclass(db0_fixture):
    obj_1 = MemoDataClass("Some Title", "Some Author", 2022, True)
    assert obj_1.title == "Some Title"
    assert obj_1.author == "Some Author"
    assert obj_1.year == 2022
    assert obj_1.is_available == True
    assert db0.is_memo(obj_1)

    
def test_memo_dataclass_kwargs_init(db0_fixture):
    obj_1 = MemoDataClass(title = "Some Title", author = "Some Author", year = 1976, is_available = False)
    assert obj_1.title == "Some Title"
    assert obj_1.author == "Some Author"
    assert obj_1.year == 1976
    assert obj_1.is_available == False
    assert db0.is_memo(obj_1)


def test_memo_dataclass_default_args(db0_fixture):
    obj_1 = MemoDataClass("Some Title", "Some Author")
    assert obj_1.title == "Some Title"
    assert obj_1.author == "Some Author"
    assert obj_1.year == 2024
    assert obj_1.is_available == True
    assert db0.is_memo(obj_1)

    
def test_memo_dataclass_default_generated_args(db0_fixture):
    obj_1 = MemoDataClassGenArgs(event_type = "Some Event")
    assert obj_1.event_type == "Some Event" 
    assert obj_1.event_name == "a_b"
    
    
def test_memo_dataclass_default_args_factory(db0_fixture):
    obj_1 = MemoDataClassArgsFactory(event_type = "Some Event")
    assert obj_1.event_type == "Some Event"
    # less than 1ms diff
    assert abs((obj_1.event_name - datetime.now()).total_seconds()) < 0.001


def test_memo_dataclass_type_passed_dynamically(db0_fixture):
    def factory(the_type, **kwargs):
        return the_type(**kwargs)
    
    obj_1 = factory(MemoDataClassGenArgs, event_type = "Some Event")
    assert obj_1.event_type == "Some Event"
