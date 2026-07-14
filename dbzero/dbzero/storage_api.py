# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from collections import namedtuple
from typing import Iterator, Any
from .dbzero import _get_prefixes, _get_current_prefix, _get_prefix_of, _get_mutable_prefixes


PrefixMetaData = namedtuple("PrefixMetaData", ["name", "uuid"])

def get_prefixes() -> Iterator[PrefixMetaData]:
    """Return all prefixes accessible from the current context.

    Returns
    -------
    Iterator[PrefixMetaData]
        An iterator that yields metadata objects for each discovered prefix.

    Examples
    --------
    Discovering and opening all available prefixes:

    >>> # First, initialize the dbzero environment
    >>> dbzero.init(dbzero_root="/path/to/my/data")
    >>> 
    >>> # Discover all available prefixes
    >>> all_prefixes = dbzero.get_prefixes()
    >>> print(f"Discovered {len(list(all_prefixes))} prefixes.")
    >>> 
    >>> # Iterate and open each one
    >>> for prefix in dbzero.get_prefixes():
    ...     print(f"Opening prefix: {prefix.name}")
    ...     dbzero.open(prefix.name, "r")
    >>> 
    >>> # Now all prefixes are open and their data can be queried
    """
    for prefix in _get_prefixes():
        yield PrefixMetaData(*prefix)


def get_mutable_prefixes() -> Iterator[PrefixMetaData]:
    """Return prefixes currently open in read-write mode.

    Returns
    -------
    Iterator[PrefixMetaData]
        An iterator of PrefixMetaData objects, where each object corresponds to a prefix 
        that is currently open in read-write ('rw') mode.

    Examples
    --------
    Listing all mutable prefixes:
    
    >>> # The default prefix is open and mutable
    >>> print([p.name for p in dbzero.get_mutable_prefixes()])
    >>> ['main']
    >>> 
    >>> # Open two more prefixes
    >>> dbzero.open('prefix1')
    >>> dbzero.open('prefix2')
    >>> 
    >>> # The list will now include all three open prefixes
    >>> print([p.name for p in dbzero.get_mutable_prefixes()])
    >>> ['main', 'prefix1', 'prefix2']
    """
    for prefix in _get_mutable_prefixes():
        yield PrefixMetaData(*prefix)


def get_current_prefix() -> PrefixMetaData:
    """Retrieve the currently active prefix.

    Returns
    -------
    PrefixMetaData
        A PrefixMetaData object that represents the current prefix.

    Examples
    --------
    Getting the current prefix name:
    
    >>> # Assuming a connection is open with a default prefix like 'main'
    >>> current_px = dbzero.get_current_prefix()
    >>> print(current_px.name)
    >>> 'main'

    How dbzero.open() and dbzero.close() affect the current prefix:
    
    >>> # Get the initial prefix
    >>> initial_prefix = dbzero.get_current_prefix()
    >>> print(f"Initial prefix: {initial_prefix.name}")
    >>> 
    >>> # Open a new prefix, which becomes the current one
    >>> dbzero.open("secondary-prefix")
    >>> print(f"New current prefix: {dbzero.get_current_prefix().name}")
    >>> 
    >>> # Close the new prefix
    >>> dbzero.close("secondary-prefix")
    >>> 
    >>> # The current prefix is restored to the initial one
    >>> print(f"Restored prefix: {dbzero.get_current_prefix().name}")
    >>> # Expected output:
    >>> # Initial prefix: main
    >>> # New current prefix: secondary-prefix
    >>> # Restored prefix: main
    """
    return PrefixMetaData(*_get_current_prefix())


def get_prefix_of(obj: Any) -> PrefixMetaData:
    """Return the prefix where given dbzero-managed object resides.

    Parameters
    ----------
    obj : Any
        The dbzero item whose prefix you want to find. This can be an object instance, 
        a class decorated with @dbzero.memo, an enum, or a dbzero query object.

    Returns
    -------
    PrefixMetaData
        A PrefixMetaData object representing the objects' prefix.

    Examples
    --------
    Getting the prefix of an object instance:
    
    >>> # Create an object on the default prefix
    >>> obj_1 = MemoTestClass(100)
    >>> print(f"obj_1 lives on prefix: {dbzero.get_prefix_of(obj_1).name}")
    >>> 
    >>> # Open a new prefix, making it the current one
    >>> dbzero.open("secondary-db")
    >>> obj_2 = MemoTestClass(200)
    >>> print(f"obj_2 lives on prefix: {dbzero.get_prefix_of(obj_2).name}")
    >>> # Expected output:
    >>> # obj_1 lives on prefix: main
    >>> # obj_2 lives on prefix: secondary-db

    Getting the prefix of a class type:
    
    >>> @dbzero.memo(prefix="scoped-class-prefix")
    ... class ScopedDataClass:
    ...     def __init__(self, value):
    ...         self.value = value
    >>> 
    >>> # Get the prefix directly from the type
    >>> class_prefix = dbzero.get_prefix_of(ScopedDataClass)
    >>> print(f"ScopedDataClass belongs to: {class_prefix.name}")
    >>> 
    >>> # An instance of the class will belong to the same prefix
    >>> instance = ScopedDataClass(42)
    >>> instance_prefix = dbzero.get_prefix_of(instance)
    >>> print(f"An instance belongs to: {instance_prefix.name}")
    >>> # Expected output:
    >>> # ScopedDataClass belongs to: scoped-class-prefix
    >>> # An instance belongs to: scoped-class-prefix
    """
    return PrefixMetaData(*_get_prefix_of(obj))
