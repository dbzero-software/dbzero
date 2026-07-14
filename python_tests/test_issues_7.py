# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
from .conftest import DB0_DIR


STATIC_DATA_CODES = {
    "B": {
        "LPL": {
            ("BFO", "Bonus za piersze zamówienie",
            "Bonus udzielany klientowi (zwykle w postaci punktów lojalnościowych) za złożenie pierwszego zamówienia"),
            ("BPB", "Bonus za wielkość zamówienia", "Bonus udzielany proporcjonalnie do wielkości zamówienia")
        },
    },
    "D": {
        "LPL": {
            ("DDD", "Rabat za długość zamówienia",
             "Typ rabatu powiązany z długością trwania zamówienia"),
            ("DLP", "Rabat za punkty ze skarbonki",
                "Rabat udzielony za wykorzystanie punktów ze skarbonki podczas płatności"),
            ("DVD", "Rabat za ilość", "Typ rabatu powiązany z wielkością zamówienia")
        },
        "LEN": {
            ("DDD", "Duration Discount", "Discount type based on order's duration"),
            ("DLP", "Loyalty points discount",
             "Discount type associated with use of loyalty points during payment"),
            ("DVD", "Volume Discount", "Discount type based on order size")
        },
    },
}

@db0.memo()
class DataCodes:
    def __init__(self):
        self._validation_errors: dict = {}
        self._data_codes = STATIC_DATA_CODES

@db0.memo()
class District:
    def __init__(self, name):
        self.name = name


def test_find_issue_2(db0_fixture):
    """
    Issue: test was failing with assertion failure
    Resolution: TagIndex revert-ops weere not cleared and 2 objects got the same address over time
    """
    DataCodes()
    db0.commit()
    obj = District(0)
    db0.tags(obj).add("test_tag")
    assert len(db0.find(District)) == 1


def test_invalid_address_when_select_modified(db0_fixture):
    DataCodes()
    db0.commit()
    snap_1 = db0.snapshot()
    obj = District("some_District")
    uuid = db0.uuid(obj)
    db0.tags(obj).add("test_tag")
    db0.commit()
    snap_2 = db0.snapshot()
    
    results = db0.select_new(db0.find(District), snap_1, snap_2)
    assert len(results) == 1
    
    obj.name = "some_District_2"
    db0.commit()
    snap_3 = db0.snapshot()
    results = db0.select_modified(db0.find(District), snap_2, snap_3)
    assert len(results) == 1
    result = next(iter(results))    
    assert result[1].name == "some_District_2"
    assert result[0].name == "some_District"
