# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

import pytest
import dbzero as db0
from .memo_test_types import MemoTestClass


@db0.enum(values=["RED", "GREEN", "BLUE"])
class Color:
    pass


def test_enum_can_be_defined_by_class(db0_fixture):
    assert str(Color.RED) == "RED"
