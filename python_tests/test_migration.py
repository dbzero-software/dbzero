# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
import datetime

@db0.memo
class MigrationTestClass:
    def __init__(self):
        self.int_value = 931
        self.str_value = "abdc"
        self.date_value = datetime.date(2023, 1, 1)
        self.list_value = []
        self.dict_value = {}
        self.set_value = set()


@db0.memo
class MigrationTestClassBase:
    def __init__(self):
        self.int_value = 931
        self.str_value = "abdc"
        self.date_value = datetime.date(2023, 1, 1)
        self.list_value = []
        self.dict_value = {}
        self.set_value = set()

@db0.memo
class MigrationTestClassDerived(MigrationTestClassBase):
    def __init__(self):
        super().__init__()
        self.ext_int_value = 123
        self.ext_str_value = "abcd"
        self.ext_date_value = datetime.date(2023, 1, 1)


def test_change_field_name_inside_class(db0_fixture):
    def new_init(self):
        self.int_value = 122
        # NOTE: this field name is changed
        self.str_new_value = "defg"
        self.date_value = datetime.date(2025, 1, 1)
        self.list_value = []
        self.dict_value = {}
        self.set_value = set()

    # Create an instance of the class
    obj_1 = MigrationTestClass()
    assert obj_1.int_value == 931
    assert obj_1.str_value == "abdc"
    assert obj_1.date_value == datetime.date(2023, 1, 1)
    
    # migrate the class by changing the __init__ method
    MigrationTestClass.__init__ = new_init
    obj_2 = MigrationTestClass()
    assert obj_2.int_value == 122
    assert obj_2.str_new_value == "defg"
    assert obj_2.date_value == datetime.date(2025, 1, 1)
    
        
def test_change_field_name_inside_derived_class(db0_fixture):
    def new_init(self):
        super(self.__class__, self).__init__()
        self.ext_int_value = 122
        self. _ext_str_new_value = "defg"
        self.ext_date_value = datetime.date(2025, 1, 1)
    
    # Create an instance of the class
    obj_1 = MigrationTestClassDerived()
    assert obj_1.int_value == 931
    assert obj_1.str_value == "abdc"
    assert obj_1.ext_int_value == 123
    assert obj_1.ext_str_value == "abcd"
    
    # migrate the class by changing the __init__ method
    MigrationTestClassDerived.__init__ = new_init
    obj_2 = MigrationTestClassDerived()
    assert obj_2.ext_int_value == 122
    assert obj_2._ext_str_new_value == "defg"
    assert obj_2.ext_date_value == datetime.date(2025, 1, 1)
    