# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from __future__ import annotations

import asyncio
import contextvars
import threading
import weakref
from dataclasses import dataclass
from typing import Any, Dict, Optional
from .interfaces import Memo
from .dbzero import begin_atomic, begin_async_atomic, assign


_async_atomic_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = weakref.WeakKeyDictionary()
_async_atomic_locks_guard = threading.Lock()


@dataclass(frozen=True)
class _AsyncAtomicState:
    owner_task: asyncio.Task
    depth: int
    lock: asyncio.Lock


_async_atomic_state: contextvars.ContextVar[Optional[_AsyncAtomicState]] = contextvars.ContextVar(
    "dbzero_async_atomic_state",
    default=None,
)


def _current_async_task() -> Optional[asyncio.Task]:
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


def _get_async_atomic_lock(loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
    with _async_atomic_locks_guard:
        lock = _async_atomic_locks.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            _async_atomic_locks[loop] = lock
        return lock


class AtomicManager:
    """Context manager that provides atomic context functionality for dbzero operations.

    It is intended for use in a 'with' statement. 
    """

    def __init__(self):
        self.__ctx = None

    def __enter__(self) -> AtomicManager:
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.close()
        else:
            self.cancel()

    def begin(self):
        """Begin the atomic context"""
        if self.__ctx is None:
            self.__ctx = begin_atomic()

    def close(self):
        """Close the atomic context, staging the changes for commit"""
        if self.__ctx is None:
            return

        self.__ctx.close()
        self.__ctx = None

    def cancel(self):
        """Cancel the atomic context, reverting all changes"""
        if self.__ctx is None:
            return

        self.__ctx.cancel()
        self.__ctx = None


def atomic() -> AtomicManager:
    """Open a context manager to group multiple mutating operations into a single indivisible transaction.

    This function ensures that all modifications within  the `with` block are applied together, or none are applied at all.
    If the block completes successfully, all changes are merged into the current 
    transaction. If an exception occurs inside the block, or if the transaction is 
    manually canceled, all changes are reverted, leaving the data in its previous state.

    Returns
    -------
    AtomicManager
        A context manager that provides atomic context functionality.

    Examples
    --------
    Grouping successful operations:
    
    >>> obj1 = MyMemoClass("initial value")
    >>> with dbzero.atomic():
    ...     obj1.value = "updated value"
    ...     obj2 = MyMemoClass("new object")
    ...     dbzero.tags(obj2).add("new")
    >>> # Both changes are now visible
    >>> assert obj1.value == "updated value"

    Automatic rollback on exception:
    
    >>> obj = MyMemoClass(value=100)
    >>> try:
    ...     with dbzero.atomic():
    ...         obj.value = 200  # This change will be reverted
    ...         raise ValueError("Something went wrong")
    ... except ValueError:
    ...     print("Caught expected error.")
    >>> # The object's value is unchanged
    >>> assert obj.value == 100

    Manual rollback with cancel():
    
    >>> obj = MyMemoClass(value=100)
    >>> with dbzero.atomic() as atomic:
    ...     obj.value = 200
    ...     if obj.value > 150:
    ...         print("Value too high, canceling.")
    ...         atomic.cancel()
    >>> # The changes were discarded
    >>> assert obj.value == 100

    Notes
    -----
    An atomic() block does not immediately create a new, committed transaction or 
    increment the global state number. Instead, the changes are staged 
    and applied as part of the surrounding transaction, which is then committed 
    either manually via dbzero.commit() or by the autocommit mechanism.
    """
    return AtomicManager()


class AsyncAtomicManager:
    """Async context manager for dbzero atomic operations in asyncio tasks."""

    def __init__(self):
        self.__ctx = None
        self.__state_token = None
        self.__lock = None
        self.__release_lock = False

    async def __aenter__(self) -> AsyncAtomicManager:
        task = _current_async_task()
        if task is None:
            raise RuntimeError("db0.async_atomic requires a running asyncio task")

        state = _async_atomic_state.get()
        if state is not None and state.owner_task is task:
            self.__lock = state.lock
            next_state = _AsyncAtomicState(task, state.depth + 1, state.lock)
        else:
            loop = asyncio.get_running_loop()
            self.__lock = _get_async_atomic_lock(loop)
            await self.__lock.acquire()
            self.__release_lock = True
            next_state = _AsyncAtomicState(task, 1, self.__lock)

        try:
            self.__ctx = begin_async_atomic()
        except BaseException:
            if self.__release_lock and self.__lock is not None:
                self.__lock.release()
            self.__release_lock = False
            self.__lock = None
            raise

        self.__state_token = _async_atomic_state.set(next_state)
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        try:
            if exc_type is None:
                self.close()
            else:
                self.cancel()
        finally:
            if self.__state_token is not None:
                _async_atomic_state.reset(self.__state_token)
                self.__state_token = None
            if self.__release_lock and self.__lock is not None:
                self.__lock.release()
            self.__release_lock = False
            self.__lock = None

    def close(self):
        """Close the atomic context, staging the changes for commit."""
        if self.__ctx is None:
            return

        self.__ctx.close()
        self.__ctx = None

    def cancel(self):
        """Cancel the atomic context, reverting all changes."""
        if self.__ctx is None:
            return

        self.__ctx.cancel()
        self.__ctx = None


def async_atomic() -> AsyncAtomicManager:
    """Open an asyncio-aware atomic context manager for dbzero operations."""
    if _current_async_task() is None:
        raise RuntimeError("db0.async_atomic requires a running asyncio task")
    return AsyncAtomicManager()


def atomic_assign(*objects: Memo, **attributes: Dict[str, Any]) -> None:
    """Perform bulk attribute updates on one or more Memo objects within an atomic transaction.

    This is a helper function that performs `dbzero.assign` operation in an atomic context.

    Parameters
    ----------
    *objects : Any
        A variable number of Memo objects to modify.
    **attributes : Dict[str, Any]
        The attributes to update, provided as name=value pairs where each key is 
        the name of an attribute to update and the corresponding value is the new 
        value to assign.
    """
    with atomic():
        assign(*objects, **attributes)
