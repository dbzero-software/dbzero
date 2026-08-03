# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

import gc

import dbzero as db0

from .memo_test_types import MemoTestClass


def test_nested_atomic_lifecycle_previous_pytest_frame(db0_fixture):
    obj = MemoTestClass([1])

    with db0.atomic():
        obj.value.append(2)
        try:
            with db0.atomic():
                obj.value.append(3)
                raise RuntimeError("nested failure")
        except RuntimeError:
            pass
        obj.value.append(4)

    assert list(obj.value) == [1, 2, 4]


def test_deep_nested_atomic_lifecycle_after_previous_test(db0_fixture):
    items = db0.list()
    index = db0.index()
    keep = MemoTestClass("keep")
    drop = MemoTestClass("drop")
    root = MemoTestClass({"drop": drop, "counter": 0})
    drop = None

    items.append(keep)
    index.add(1, keep)
    db0.tags(keep).add("nested-keep")

    with db0.atomic():
        root.value["counter"] = 1
        committed_child = MemoTestClass("child-commit")
        items.append(committed_child)
        index.add(2, committed_child)
        db0.tags(committed_child).add("nested-child-commit")

        try:
            with db0.atomic():
                nonlocal_marker = MemoTestClass("child-rollback")
                root.value["counter"] = 999
                root.value["drop"] = None
                items.append(nonlocal_marker)
                index.add(3, nonlocal_marker)
                db0.tags(nonlocal_marker).add("nested-child-rollback")

                with db0.atomic():
                    root.value["counter"] = 1000
                    raise RuntimeError("level 3 rollback")
        except RuntimeError:
            nonlocal_marker = None

        assert root.value["counter"] == 1
        assert [obj.value for obj in items] == ["keep", "child-commit"]
        assert {obj.value for obj in index.select(0, 10)} == {"keep", "child-commit"}
        assert len(list(db0.find("nested-child-rollback"))) == 0
        assert root.value["drop"].value == "drop"

        with db0.atomic():
            root.value["counter"] = 2
            grandchild = MemoTestClass("grandchild-commit")
            items.append(grandchild)
            index.add(4, grandchild)
            db0.tags(grandchild).add("nested-grandchild-commit")

        root.value["drop"] = None

    assert root.value["counter"] == 2
    assert [obj.value for obj in items] == [
        "keep",
        "child-commit",
        "grandchild-commit",
    ]
    assert {obj.value for obj in index.select(0, 10)} == {
        "keep",
        "child-commit",
        "grandchild-commit",
    }
    assert len(list(db0.find("nested-keep"))) == 1
    assert len(list(db0.find("nested-child-commit"))) == 1
    assert len(list(db0.find("nested-grandchild-commit"))) == 1
    assert len(list(db0.find("nested-child-rollback"))) == 0
    assert root.value["drop"] is None

    root = items = index = keep = committed_child = grandchild = None
    gc.collect()
