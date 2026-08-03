# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

import gc
import os
import shutil
import subprocess
import sys
import textwrap
from contextvars import ContextVar

import pytest
import dbzero as db0

from .conftest import DB0_DIR


@db0.memo
class RestrictedMemoAccessData:
    def __init__(self, value):
        self.value = value

    def get_value(self):
        return self.value

    def set_value(self, value):
        self.value = value


def _clean_db0_dir():
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)
    os.mkdir(DB0_DIR)


@pytest.fixture()
def db0_restricted_root():
    if "D" in db0.build_flags():
        db0.reset_test_params()
    _clean_db0_dir()
    yield
    gc.collect()
    db0.close()
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)


def _assert_restricted(obj, *, writable=True):
    assert obj.value == 123
    if writable:
        obj.value = 456
        assert obj.value == 456
        obj.new_value = 789
        assert obj.new_value == 789

    for attr_name in (
        "__class__",
        "__dict__",
        "__mro__",
        "__subclasses__",
        "_private",
    ):
        with pytest.raises(AttributeError):
            getattr(obj, attr_name)


def _assert_restricted_method_not_introspectable(method):
    assert callable(method)
    with pytest.raises(AttributeError):
        dir(method)
    for attr_name in ("__class__", "__dict__", "__func__", "__self__", "__globals__", "__closure__"):
        with pytest.raises(AttributeError):
            getattr(method, attr_name)


def test_restricted_memo_methods_can_be_called(db0_restricted_root):
    db0.init(DB0_DIR)
    db0.open("restricted-method-prefix", restricted=True)
    obj = RestrictedMemoAccessData(123)

    assert obj.get_value() == 123
    obj.set_value(456)
    assert obj.value == 456
    assert obj.get_value() == 456
    _assert_restricted_method_not_introspectable(obj.get_value)
    _assert_restricted_method_not_introspectable(obj.set_value)


def test_init_restricted_applies_to_future_prefixes(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    assert db0.get_config()["restricted"] is True
    db0.open("restricted-by-init")
    assert db0.get_prefix_stats()["restricted"] is True

    _assert_restricted(RestrictedMemoAccessData(123))


def test_open_restricted_is_scoped_to_prefix(db0_restricted_root):
    db0.init(DB0_DIR)
    assert db0.get_config()["restricted"] is False

    db0.open("restricted-prefix", restricted=True)
    assert db0.get_prefix_stats("restricted-prefix")["restricted"] is True
    restricted = RestrictedMemoAccessData(123)
    _assert_restricted(restricted)

    db0.open("unrestricted-prefix")
    assert db0.get_prefix_stats("unrestricted-prefix")["restricted"] is False
    unrestricted = RestrictedMemoAccessData(123)
    assert unrestricted.get_value() == 123
    assert unrestricted.__class__ is RestrictedMemoAccessData
    assert unrestricted.get_value.__self__ is unrestricted


def test_unrestricted_memo_access_before_restricted_mode_is_ever_used():
    code = textwrap.dedent(
        """
        import gc
        import os
        import shutil

        import dbzero as db0

        from python_tests.conftest import DB0_DIR
        from python_tests.test_restricted_memo_access import (
            RestrictedMemoAccessData,
            _clean_db0_dir,
        )

        if "D" in db0.build_flags():
            db0.reset_test_params()
        _clean_db0_dir()
        try:
            db0.init(DB0_DIR)
            db0.open("never-restricted-prefix")
            obj = RestrictedMemoAccessData(123)

            assert obj.value == 123
            assert obj.get_value() == 123
            assert obj.__class__ is RestrictedMemoAccessData
            assert obj.get_value.__self__ is obj
        finally:
            gc.collect()
            db0.close()
            if os.path.exists(DB0_DIR):
                shutil.rmtree(DB0_DIR)
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=os.getcwd(),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, (
        "restricted memo fast-path fresh-process child failed with code "
        f"{result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_unrestricted_prefix_stays_unrestricted_after_default_restricted_was_enabled(db0_restricted_root):
    db0.init(DB0_DIR, restricted=True)
    assert db0.get_config()["restricted"] is True
    db0.open("explicitly-unrestricted-prefix", restricted=False)
    assert db0.get_prefix_stats("explicitly-unrestricted-prefix")["restricted"] is False

    obj = RestrictedMemoAccessData(123)
    assert obj.value == 123
    assert obj.get_value() == 123
    assert obj.__class__ is RestrictedMemoAccessData
    assert obj.get_value.__self__ is obj


def test_prefix_can_be_reopened_as_restricted(db0_restricted_root):
    db0.init(DB0_DIR)
    db0.open("reopen-prefix", restricted=False)
    obj = RestrictedMemoAccessData(123)
    obj_uuid = db0.uuid(obj)
    assert obj.get_value() == 123
    db0.commit()
    db0.close("reopen-prefix")

    db0.open("reopen-prefix", "r", restricted=True)
    assert db0.get_prefix_stats("reopen-prefix")["restricted"] is True
    reopened = db0.fetch(obj_uuid)
    _assert_restricted(reopened, writable=False)


def test_global_restricted_applies_to_auto_opened_prefix(db0_restricted_root):
    db0.init(DB0_DIR)
    db0.open("auto-open-prefix")
    obj = RestrictedMemoAccessData(123)
    obj_uuid = db0.uuid(obj)
    db0.commit()
    db0.close()

    db0.init(DB0_DIR, restricted=True)
    fetched = db0.fetch(obj_uuid)
    assert db0.get_config()["restricted"] is True
    assert db0.get_prefix_stats("auto-open-prefix")["restricted"] is True
    _assert_restricted(fetched, writable=False)


def test_global_restricted_context_controls_existing_objects(db0_restricted_root):
    restricted_context = ContextVar("restricted_context", default=False)
    db0.init(DB0_DIR, restricted_context=restricted_context)
    assert db0.get_config()["restricted"] is False
    db0.open("dynamic-global-prefix")
    assert db0.get_prefix_stats("dynamic-global-prefix")["restricted"] is False
    obj = RestrictedMemoAccessData(123)

    assert obj.__class__ is RestrictedMemoAccessData
    assert obj.get_value.__self__ is obj

    token = restricted_context.set(True)
    try:
        _assert_restricted(obj)
    finally:
        restricted_context.reset(token)

    assert obj.__class__ is RestrictedMemoAccessData
    assert obj.get_value.__self__ is obj


def test_prefix_restricted_context_overrides_global_context(db0_restricted_root):
    global_context = ContextVar("global_context", default=True)
    prefix_context = ContextVar("prefix_context", default=False)
    db0.init(DB0_DIR, restricted_context=global_context)

    db0.open("global-dynamic-prefix")
    global_obj = RestrictedMemoAccessData(123)

    db0.open("prefix-dynamic-prefix", restricted_context=prefix_context)
    prefix_obj = RestrictedMemoAccessData(123)

    _assert_restricted(global_obj)
    assert prefix_obj.__class__ is RestrictedMemoAccessData
    assert prefix_obj.get_value.__self__ is prefix_obj

    token = prefix_context.set(True)
    try:
        _assert_restricted(prefix_obj)
    finally:
        prefix_context.reset(token)


def test_prefix_restricted_context_none_cannot_replace_existing_context(db0_restricted_root):
    global_context = ContextVar("global_context", default=True)
    prefix_context = ContextVar("prefix_context", default=False)
    db0.init(DB0_DIR, restricted_context=global_context)
    db0.open("prefix-reset-dynamic-prefix", restricted_context=prefix_context)
    obj = RestrictedMemoAccessData(123)

    assert obj.__class__ is RestrictedMemoAccessData
    assert obj.get_value.__self__ is obj

    with pytest.raises(RuntimeError):
        db0.open("prefix-reset-dynamic-prefix", restricted_context=None)

    assert obj.__class__ is RestrictedMemoAccessData


def test_static_restricted_without_restricted_context_remains_restricted(db0_restricted_root):
    db0.init(DB0_DIR)
    db0.open("static-restricted-prefix", restricted=True)
    assert db0.get_prefix_stats("static-restricted-prefix")["restricted"] is True

    _assert_restricted(RestrictedMemoAccessData(123))


def test_static_restricted_object_does_not_resolve_later_restricted_context(db0_restricted_root):
    class ExplodingContext:
        def get(self):
            raise AssertionError("restricted_context should not be resolved")

    db0.init(DB0_DIR)
    db0.open("static-no-context-prefix", restricted=True)
    obj = RestrictedMemoAccessData(123)

    with pytest.raises(RuntimeError):
        db0.open("static-no-context-prefix", restricted_context=ExplodingContext())

    _assert_restricted(obj)


def test_unset_restricted_context_is_treated_as_false(db0_restricted_root):
    restricted_context = ContextVar("restricted_context")
    db0.init(DB0_DIR, restricted_context=restricted_context)
    db0.open("unset-dynamic-prefix")
    obj = RestrictedMemoAccessData(123)

    assert obj.__class__ is RestrictedMemoAccessData
    assert obj.get_value.__self__ is obj


def test_restricted_context_uses_python_truthiness(db0_restricted_root):
    restricted_context = ContextVar("restricted_context", default=[])
    db0.init(DB0_DIR, restricted_context=restricted_context)
    db0.open("truthy-dynamic-prefix")
    obj = RestrictedMemoAccessData(123)

    assert obj.__class__ is RestrictedMemoAccessData

    token = restricted_context.set([object()])
    try:
        _assert_restricted(obj)
    finally:
        restricted_context.reset(token)


def test_init_prefix_shortcut_forwards_restricted_context(db0_restricted_root):
    restricted_context = ContextVar("restricted_context", default=True)
    db0.init(DB0_DIR, prefix="shortcut-dynamic-prefix", restricted_context=restricted_context)
    assert db0.get_prefix_stats("shortcut-dynamic-prefix")["restricted"] is False

    _assert_restricted(RestrictedMemoAccessData(123))


def test_set_restricted_context_upgrades_existing_and_future_prefixes(db0_restricted_root):
    restricted_context = ContextVar("restricted_context", default=False)
    db0.init(DB0_DIR)
    db0.open("dynamic-upgrade-prefix")
    obj = RestrictedMemoAccessData(123)

    assert obj.__class__ is RestrictedMemoAccessData
    db0.set_restricted(restricted_context=restricted_context)
    assert db0.get_config()["restricted"] is False

    assert obj.__class__ is RestrictedMemoAccessData
    token = restricted_context.set(True)
    try:
        _assert_restricted(obj)
        db0.open("future-dynamic-upgrade-prefix")
        _assert_restricted(RestrictedMemoAccessData(123))
    finally:
        restricted_context.reset(token)


def test_set_restricted_static_upgrades_dynamic_context(db0_restricted_root):
    restricted_context = ContextVar("restricted_context", default=False)
    db0.init(DB0_DIR)
    db0.set_restricted(restricted_context=restricted_context)
    db0.open("dynamic-to-static-prefix")
    obj = RestrictedMemoAccessData(123)

    assert obj.__class__ is RestrictedMemoAccessData
    db0.set_restricted(restricted=True)

    _assert_restricted(obj)
    db0.open("future-static-upgrade-prefix")
    assert db0.get_prefix_stats("future-static-upgrade-prefix")["restricted"] is True
    _assert_restricted(RestrictedMemoAccessData(123))


def test_set_restricted_rejects_context_after_static_restricted(db0_restricted_root):
    restricted_context = ContextVar("restricted_context", default=False)
    db0.init(DB0_DIR, restricted=True)
    db0.open("static-terminal-prefix")
    obj = RestrictedMemoAccessData(123)

    with pytest.raises(RuntimeError):
        db0.set_restricted(restricted_context=restricted_context)

    _assert_restricted(obj)


def test_set_restricted_rejects_context_replacement(db0_restricted_root):
    first_context = ContextVar("first_context", default=False)
    second_context = ContextVar("second_context", default=True)
    db0.init(DB0_DIR)
    db0.set_restricted(restricted_context=first_context)
    db0.open("context-replacement-prefix")
    obj = RestrictedMemoAccessData(123)

    with pytest.raises(RuntimeError):
        db0.set_restricted(restricted_context=second_context)

    assert obj.__class__ is RestrictedMemoAccessData
    token = first_context.set(True)
    try:
        _assert_restricted(obj)
    finally:
        first_context.reset(token)


def test_set_restricted_rejects_false(db0_restricted_root):
    db0.init(DB0_DIR)

    with pytest.raises(RuntimeError):
        db0.set_restricted(restricted=False)


def test_set_restricted_prefix_only_upgrades_target_prefix(db0_restricted_root):
    restricted_context = ContextVar("restricted_context", default=True)
    db0.init(DB0_DIR)

    db0.open("target-prefix")
    target = RestrictedMemoAccessData(123)
    db0.open("other-prefix")
    other = RestrictedMemoAccessData(123)

    db0.set_restricted(prefix="target-prefix", restricted_context=restricted_context)

    _assert_restricted(target)
    assert other.__class__ is RestrictedMemoAccessData
    assert other.get_value.__self__ is other
