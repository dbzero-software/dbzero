# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0


@db0.memo
class MemoFlexInit:
    def __init__(self, init_func):
        init_func(self)


def test_memo_lofi_read_on_init(db0_fixture):
    def init_func(obj):
        obj.val_1 = False
        obj.val_2 = True
        obj.val_3 = None
        
        assert obj.val_1 == False
        assert obj.val_2 == True
        assert obj.val_3 is None

    _  = MemoFlexInit(init_func)


def test_memo_lofi_update_on_init(db0_fixture):
    def init_func(obj):
        obj.val_1 = False
        obj.val_2 = True
        obj.val_3 = None
        assert obj.val_1 == False
        # update from lo-fi to regular
        obj.val_1 = 12345
        
        assert obj.val_1 == 12345

    obj_1 = MemoFlexInit(init_func)
    assert obj_1.val_1 == 12345
    assert obj_1.val_2 == True
    assert obj_1.val_3 is None


def test_memo_update_to_lofi_on_init(db0_fixture):
    def init_func(obj):
        obj.val_1 = 12435
        obj.val_2 = True
        obj.val_3 = None
        assert obj.val_1 == 12435
        # update from regular to lo-fi
        obj.val_1 = True
        
        assert obj.val_1 == True

    obj_1 = MemoFlexInit(init_func)
    assert obj_1.val_1 == True
    assert obj_1.val_2 == True
    assert obj_1.val_3 is None
    # make sure only 1 member slot was allocated
    assert len(db0.describe(obj_1)["field_layout"]["pos_vt"]) == 1
