# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from .conftest import DB0_DIR


@db0.memo(singleton=True)
class TestDynScopedSingleton:
    __test__ = False
    def __init__(self, prefix=None, value=None):
        db0.set_prefix(self, prefix)
        self.value = value


def test_dyn_scope_ignored_if_none(db0_fixture):
    object = TestDynScopedSingleton(prefix=None)
    assert db0.get_prefix_of(object).name == db0.get_current_prefix().name
    

def test_dyn_scope_not_allowed_after_instantiation(db0_fixture):
    object = TestDynScopedSingleton()
    with pytest.raises(Exception):
        db0.set_prefix(object, "scoped-class-prefix")    


def test_scope_defined_dynamically_for_a_singleton(db0_fixture):
    object = TestDynScopedSingleton("scoped-class-prefix")
    assert db0.get_prefix_of(object).name == "scoped-class-prefix"


def test_opening_dyn_scoped_singleton(db0_fixture):
    object = TestDynScopedSingleton("scoped-class-prefix", 123)
    del object
    object = TestDynScopedSingleton(prefix="scoped-class-prefix")
    assert db0.get_prefix_of(object).name == "scoped-class-prefix"
    assert object.value == 123
    
