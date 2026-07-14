# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from datetime import datetime
from .memo_test_types import MemoTestClass
import random


@db0.memo
class Basket:
    def __init__(self, client):
        self.client = client
        self.items = []


@db0.memo
class Client:
    def __init__(self, first_name, last_name, email, phone):
        self.first_name = first_name
        self.last_name = last_name
        self.email = email
        self.phone = phone
        self.calendar = None
        # self.basket = Basket(self)
        self.addresses = []
        self.primary_address = None
        # order history
        self.orders = []
        # orders pending payment
        self.unpaid_orders = db0.set()
        self.canceled_orders = db0.set()
        self.plan_history = []
        self.active_diet_plans = db0.set()
        self.basket = Basket(self)


def test_create_memo_with_back_reference_issue_1(db0_fixture):
    """
    Issue: the test was causing a segmentation fault with exception:
    0x00007fd71d978db7 in db0::v_sorted_vector<db0::object_model::XValue, db0::object_model::KV_Address, std::less<db0::object_model::XValue> >
    ::updateExisting (this=0x2702e70, data=..., old_data=0x7fff5a870f20)
    at ../../src/dbzero/core/collections/vector/v_sorted_vector.hpp:901
        901 *old_data = *it;
    when assigning KV-member (required to register reference to self)
    Resolution: ???
    """
    client = Client("John", "Doe", "john.doe@gmail.com", "1234567890")    
    assert client.first_name == "John"


def test_list_iterator_issue_1(db0_fixture):
    buf = db0.list([1,2,3])
    for item in buf:
        assert item > 0
    

@pytest.mark.stress_test
def test_list_iterator_issue_2(db0_fixture):
    lists = []
    buf = db0.list()
    for _ in range(100):
        lists.append(db0.list([MemoTestClass(random.randint(1, 100)) for _ in range(1024)]))    
        list = random.choice(lists)
        # iterate over list items
        for item in list:
            buf.append(MemoTestClass(item.value))
            assert item.value > 0