# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from __future__ import annotations

from .dbzero import begin_read_only


class ReadOnlyManager:
    """Context manager that rejects dbzero mutations while active."""

    def __init__(self):
        self.__ctx = None

    def __enter__(self) -> ReadOnlyManager:
        self.__ctx = begin_read_only()
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        if self.__ctx is not None:
            self.__ctx.close()
            self.__ctx = None


def read_only() -> ReadOnlyManager:
    """Open a context manager that rejects dbzero mutations in its block."""
    return ReadOnlyManager()
