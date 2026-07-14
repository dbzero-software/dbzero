# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0


def test_isinstance_dict(db0_fixture):
    cut = db0.dict()
    assert isinstance(cut, db0.types.Dict)
    assert not isinstance(cut, db0.types.Set)


def test_isinstance_set(db0_fixture):
    cut = db0.set()
    assert isinstance(cut, db0.types.Set)
    assert not isinstance(cut, db0.types.Dict)


def test_isinstance_tuple(db0_fixture):
    cut = db0.tuple([0, 1, 2])    
    assert isinstance(cut, db0.types.Tuple)
    assert not isinstance(cut, db0.types.Dict)


def test_isinstance_list(db0_fixture):
    cut = db0.list([1,2,3])
    assert isinstance(cut, db0.types.List)
    assert not isinstance(cut, db0.types.Tuple)
