import pytest
import dbzero_ce as db0
from unittest.mock import patch


@db0.memo
class PrivMemberFoundation():
    def __init__(self):
        # FIXME: log
        print("PrivMemberFoundation.__init__")
        self.field_0 = 1
        self.field_1 = 2
        self.field_2 = 3
        self.field_3 = 4


@db0.memo
class PrivMemberBaseClass(PrivMemberFoundation):
    def __init__(self):
        # FIXME: log
        print("PrivMemberBaseClass.__init__")
        super().__init__()
        self.field_4 = 5
        self.field_5 = 6
        self._my_dict = {}
        self.field_6 = 7
        self.field_7 = 8
        for k, v in {"a": 1, "b": 2}.items():
            self._my_dict[k] = v
    

@db0.memo
class PrivMemberSubClass(PrivMemberBaseClass):
    def __init__(self, value):
        super().__init__()
        self._value = value

    
def test_private_member_of_subclass_issue_1(db0_fixture):
    """
    Issue description: 
    """
    px_name = db0.get_current_prefix().name
    db0.close()
    db0.open(px_name, "rw")
    obj = PrivMemberSubClass(42)
    assert obj is not None