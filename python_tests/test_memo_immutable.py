from random import random
import pytest
import dbzero as db0
from dataclasses import dataclass
from .conftest import DB0_DIR
import random


@db0.memo(immutable=True)
@dataclass
class MemoImmutableClass:
    data: str
    value: int = 0
                
def test_create_memo_immutable(db0_fixture):
    obj_1 = MemoImmutableClass(data="immutable data", value=42)
    assert obj_1.data == "immutable data"
    assert obj_1.value == 42
