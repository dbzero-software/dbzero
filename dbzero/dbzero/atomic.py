# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from __future__ import annotations

from typing import Any, Dict
from .interfaces import Memo
from .dbzero import begin_atomic, assign


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
