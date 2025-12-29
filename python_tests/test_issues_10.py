# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from .conftest import DB0_DIR
from datetime import datetime
import random


@db0.memo
class Address:
    _SEARCH_FIELDS = ('city', 'street', 'building_no', 'postal_code', 'apt_no')

    def __init__(
            self,
            ord_no: int,
            country,
            city,
            street: str,
            building_no: str,
            postal_code,
            apt_no: str = None,
            entrance_no: str = None,
            floor: str = None,
            building_access_code: str = None,
            comments: str = None,            
    ):
        self.ord_no = ord_no
        self.country = country
        self.city = city
        self.street = street
        self.building_no = building_no
        self.postal_code = postal_code
        self.apt_no = apt_no
        self.entrance_no = entrance_no
        self.floor = floor
        self.building_access_code = building_access_code
        self.comments = comments
        self.workflows_deadline = None
        self._verified = False
        self._created_at = datetime.now()
        self.address_id = 0


def test_memo_class_mutate_issue1(db0_fixture):
    def _update_func():
        addr = next(iter(db0.find(Address)))
        addr.building_access_code = "1234"
    
    address = Address(
        ord_no=0 , country="PL", city="Warsaw", street="Toruńska",
        building_no="9", postal_code="00-412", apt_no="1"
    )
    assert address.building_access_code is None
    _update_func()
    assert address.building_access_code == "1234"
    
    
@db0.memo(no_default_tags=True)
class ShoppingCart:
    def __init__(self, as_temp: bool = False):
        self.delivery_to: None
        self._items: []

        if as_temp:
            db0.tags(self).add("TEMP")
            self.__secret = random.randint(1, 1 << 32)

    @property
    def secret(self) -> int:
        return self.__secret
    
    
def test_shopping_cart_secret_issue(db0_fixture):
    cart_1 = ShoppingCart(as_temp=True)
    assert cart_1.secret is not None
    cart_2 = ShoppingCart(as_temp=False)
    assert cart_2.secret is None
    