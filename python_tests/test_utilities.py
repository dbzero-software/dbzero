# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0

def test_taggify():
    assert list(db0.taggify("--Mińsk Mazowiecki", max_len = 4)) == ['MINS', 'MAZO']
    assert list(db0.taggify("Markowski, Marek", max_len = 3)) == ['MAR']
    assert list(db0.taggify("A.Kowalski", min_len = 3)) == ['KOW']
    assert list(db0.taggify("A.Kowalski", max_len = None)) == ['A', 'KOWALSKI']
    assert list(db0.taggify("15-593", max_len = 2)) == ['15', '59']
    assert list(db0.taggify("15-593", max_len = None)) == ['15', '593']
    assert list(db0.taggify("Zażółć Gęślą Jaźń", suffix=True)) == ['OLC', 'SLA', 'AZN']
    assert list(db0.taggify("ab ba", suffix=True)) == ['AB', 'BA']