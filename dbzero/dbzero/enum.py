# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from typing import Any, Optional, List, Union, overload
from .interfaces import EnumType
from .dbzero import _make_enum


@overload
def enum(cls: str, values: List[str]) -> EnumType: ...

@overload 
def enum(*, values: List[str]) -> EnumType: ...

@overload
def enum(cls: type) -> EnumType: ...

def enum(cls: Optional[Union[str, type]] = None, *args: Any, **kwargs: Any) -> EnumType:
    """Create a persistent, type-safe enumerated type.

    Useful for defining a set of named constants.
    Using dbzero enums instead of raw strings for tags or object members prevents accidental 
    clashes and makes the data model more robust and explicit.

    Parameters
    ----------
    cls : str
        The name for the enum type. 
        When used as a decorator, the name is inferred from the class name. 
    values : list of str
        A list of unique string names for the enum members. The order of values 
        is preserved.

    Returns
    -------
    EnumType
        A new EnumType object.

    Examples
    --------
    Decorator with values:
    
    >>> @dbzero.enum(values=["PENDING", "ACTIVE", "INACTIVE"])
    ... class Status:
    ...     pass
    >>> print(Status.ACTIVE)  # "ACTIVE"

    As a function:
    
    >>> Color = dbzero.enum("Color", ["RED", "GREEN", "BLUE"])
    >>> print(Color.RED)  # "RED"

    Accessing members:
    
    >>> # Access by attribute
    >>> active_status = Status.ACTIVE
    >>> # Access by string key
    >>> red_color = Color['RED']
    >>> # Access by integer index
    >>> green_color = Color[1]  # Corresponds to "GREEN"

    Type safety for tagging:
    
    >>> Color = dbzero.enum("Color", ["RED", "GREEN", "BLUE"])
    >>> Palette = dbzero.enum("Palette", ["RED", "ORANGE", "YELLOW"])
    >>> 
    >>> # Tag different objects
    >>> dbzero.tags(obj1).add(Color.RED)
    >>> dbzero.tags(obj2).add(Palette.RED)
    >>> dbzero.tags(obj3).add("RED")
    >>> 
    >>> # Each find query returns distinct sets
    >>> list(dbzero.find(Color.RED))    # [obj1]
    >>> list(dbzero.find(Palette.RED))  # [obj2]
    >>> list(dbzero.find("RED"))        # [obj3]
    """
    def wrap(cls_):
        return _make_enum(cls_, **kwargs)
    
    # See if we're being called as @enum or @enum().
    if cls is None:
        # We're called with parens.
        return wrap

    if isinstance(cls, str):
        # enum called as a regular function.
        return _make_enum(cls, *args, **kwargs)
    
    # We're called as @enum without parens.
    return wrap(cls, **kwargs)
