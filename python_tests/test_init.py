# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

from contextvars import ContextVar
import builtins
import sys
from types import SimpleNamespace

import pytest
import dbzero as db0

from .conftest import DB0_DIR


def _install_fake_rpc(monkeypatch):
    calls = []

    def fake_init(**kwargs):
        calls.append(kwargs)

    monkeypatch.setitem(sys.modules, "db0_rpc", SimpleNamespace(init=fake_init))
    return calls


def _install_failing_rpc(monkeypatch):
    def fake_init(**kwargs):
        raise ValueError("invalid rpc config")

    monkeypatch.setitem(sys.modules, "db0_rpc", SimpleNamespace(init=fake_init))


def test_cache_size_can_be_specified_on_init(db0_fixture):
    db0.close()
    db0.init(DB0_DIR, cache_size=123456, lang_cache_size=9876)
    db0.open("my-test-prefix")    
    assert db0.get_lang_cache_stats()["capacity"] == 9876

    stats = db0.get_cache_stats()
    assert stats["capacity"] == 123456


def test_init_forwards_rpc_skip_auth(db0_fixture, monkeypatch):
    db0.close()
    calls = _install_fake_rpc(monkeypatch)

    db0.init(DB0_DIR, rpc={"dangerously_skip_auth": True})

    assert calls == [{"dangerously_skip_auth": True}]


def test_init_forwards_exact_rpc_auth_token_var(db0_fixture, monkeypatch):
    db0.close()
    calls = _install_fake_rpc(monkeypatch)
    token_var = ContextVar("rpc_auth_token")

    db0.init(DB0_DIR, rpc={"auth_token_var": token_var})

    assert calls == [{"auth_token_var": token_var}]


def test_init_rejects_invalid_rpc_config(db0_fixture):
    db0.close()

    with pytest.raises(TypeError):
        db0.init(DB0_DIR, rpc="invalid")


def test_init_without_rpc_does_not_import_db0_rpc(db0_fixture, monkeypatch):
    db0.close()
    monkeypatch.delitem(sys.modules, "db0_rpc", raising=False)
    original_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name == "db0_rpc":
            raise AssertionError("db0_rpc should not be imported when rpc is omitted")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    db0.init(DB0_DIR)


def test_init_propagates_rpc_init_error_and_allows_recovery(db0_fixture, monkeypatch):
    db0.close()
    _install_failing_rpc(monkeypatch)

    with pytest.raises(ValueError, match="invalid rpc config"):
        db0.init(DB0_DIR, rpc={"dangerously_skip_auth": True})

    db0.close()
    db0.init(DB0_DIR)
    db0.open("my-test-prefix")
