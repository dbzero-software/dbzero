# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

"""
Definitions of interfaces and types used in dbzero.

These classes serve as type annotations and documentation only.
They correspond to their C++ implementations but are not a part of actual Python API.
"""

from __future__ import annotations
from typing import Any, Optional, Union, Iterator, Iterable, List, Tuple

class Memo:
    """A dbzero.memo decorated class type."""
    ...

class MemoWeakProxy:
    """A weak reference to a Memo object that doesn't extend its lifetime."""
    ...

class EnumType:
    """A dbzero.enum object."""
    ...

class EnumValue:
    """A member of dbzero.enum."""
    ...

class QueryObject(Iterator[Memo]):
    """A dbzero objects query type."""

    def __len__(self) -> int:
        """The number of queried elements."""
        ...

    def __getitem__(self, obj: slice) -> QueryObject:
        """Get a slice of query result set."""
        ...

class Tag:
    """Memo object/class tag, EnumValue or a simple 'str' tag."""
    ...

class TagSet:
    """A tag set operation, e.g. logical complement, being a result of query negation."""
    ...

class ListObject(list):
    """Persistent list."""
    ...

class IndexObject:
    """Persistent ordered data structure for efficient queries."""

    def add(self, key: Any, value: Memo, /) -> None:
        """Associate a value with a sortable key in the index.

        Parameters
        ----------
        key : Any
            The sortable key to associate with the value. Can be any comparable type
            including None, numbers, strings or datetime objects.
        value : Memo
            A Memo object to be associated with the key.
        """
        ...

    def remove(self, key: Any, value: Memo, /) -> None:
        """Remove a specific key-value pair from the index.

        Both the key and value must match exactly for the removal to succeed.

        Parameters
        ----------
        key : Any
            The exact key that was used when adding the value.
        value : Memo
            The exact Memo object to remove.
        """
        ...

    def select(self, low: Optional[Any] = None, high: Optional[Any] = None, null_first: bool = False) -> QueryObject:
        """Query objects within a key range with inclusive bounds.

        Performs a range query returning all objects whose keys fall within the specified
        range. Both bounds are inclusive. If no arguments are provided, returns all objects.

        Parameters
        ----------
        low : Any, optional
            The minimum key value. If None, no lower bound is applied.
        high : Any, optional
            The maximum key value. If None, no upper bound is applied.
        null_first : bool, default False
            If True, None keys are considered 'less' than all other values.
            If False, None keys are considered 'greater' than all other values.

        Returns
        -------
        QueryObject
            A query object containing all values whose keys fall within [low, high].
        """
        ...

    def sort(self, query: QueryObject, /, *, desc: bool = False, null_first: bool = False) -> QueryObject:
        """Sort objects by their keys in this index.

        Takes a query result and returns the same objects sorted according to their
        keys in this index. Objects not present in the index are excluded from the result.

        Parameters
        ----------
        query : QueryObject
            A query of dbzero objects to be sorted.
        desc : bool, default False
            If True, sort in descending order. If False, sort in ascending order.
        null_first : bool, default False
            If True, None keys are considered 'less' than all other values.
            If False, None keys are considered 'greater' than all other values.

        Returns
        -------
        QueryObject
            A new query containing the input objects sorted by their keys in this index.
        """
        ...

class TupleObject(tuple):
    """Persistent immutable sequence."""
    ...

class SetObject(set):
    """Persistent unordered collection of unique elements."""
    ...

class DictObject(dict):
    """Persistent mapping object."""
    ...

class ByteArrayObject(bytearray):
    """Persisted sequence of bytes."""
    ...

class ObjectTagManager:
    """Manages tags of one or more Memo instances."""

    def add(self, *tag: Union[Tag, Iterable[Tag]]) -> None:
        """Add one or more tags to the managed objects.

        Parameters
        ----------
        *tag : Union[Tag, Iterable[Tag]]
            Tags to add. Can be individual tags as separate arguments, or collections of tags.
        """
        ...

    def remove(self, *tag: Union[Tag, Iterable[Tag]]) -> None:
        """Remove one or more tags from the managed objects.

        Parameters
        ----------
        *tag : Union[Tag, Iterable[Tag]]
            Tags to remove. Can be individual tags as separate arguments, or collections of tags.
            Tags that weren't previously assigned to managed objects are ignored.
        """
        ...

class Snapshot:
    """A dbzero snapshot context.
    
    It is intended for use in a 'with' statement.
    """

    def __enter__(self) -> Snapshot:
        """Enter dbzero snapshot context."""
        ...

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit dbzero snapshot's context."""
        ...

    def fetch(self, id: Union[str, type], type: Optional[type] = None, prefix: Optional[str] = None) -> Memo:
        """Retrieve a single object directly from memory using its unique identifier.

        Parameters
        ----------
        id : str or type
            The identifier for the object you want to retrieve.
            
            * UUID string: Returns the specific object instance for that UUID
            * type (singleton class): Returns the unique instance of that singleton
        type : type, optional
            Optional type to validate the retrieved object.
            Raises exception if the fetched object is not an instance of this type.
        prefix : str, optional
            Optional name of the data prefix to fetch the object from.
            Useful for retrieving singletons from non-default prefixes.

        Returns
        -------
        Memo
            The requested Memo object instance.

        Raises
        ------
        Exception
            If the object cannot be found or type validation fails.
        """
        ...

    def exists(identifier: Union[str, type], expected_type: Optional[type] = None, prefix: Optional[str] = None) -> bool:
        """Check if an identifier points to a valid dbzero object or an existing singleton instance

        Parameters
        ----------
        identifier : str or type
            The identifier for object to check for.
            
            * str: Check for object with its unique identifier
            * type: Check for instance of this singleton type
        expected_type : type, optional
            Optional expected type when checking by UUID.
            Verifies the found object is an instance of this type.
        prefix : str, optional
            Optional prefix name to search within. Defaults to currently active prefix.
            Only used when checking singleton types.

        Returns
        -------
        bool
            True if the object exists (and matches type if specified), False otherwise.
        """
        ...

    def find(self, *query_criteria: Union[Tag, List[Tag], Tuple[Tag], QueryObject, TagSet], prefix: Optional[str] = None) -> QueryObject:
        """Query for memo objects based on search criteria such as tags, types, or subqueries.

        Parameters
        ----------
        *query_criteria : Union[Tag, List[Tag], Tuple[Tag], QueryObject, TagSet]
            Variable number of criteria to filter objects:
            
            * Type: A class to filter by type (includes subclasses)
            * String tag: Simple string tag
            * Object tag: Any memo object used as a tag
            * List of tags (OR): Objects with at least one of the specified tags
            * Tuple of tags (AND): Objects with all of the specified tags
            * QueryObject: Result of another query
            * TagSet: Logical set operation.
        prefix : str, optional
            Optional data prefix to run the query on.
            If omitted, the prefix to run the query is resolved based on query criteria.

        Returns
        -------
        QueryObject
            An iterable query object.
        """
        ...

    def deserialize(self, data: bytes, /) -> Any:
        """Reconstruct a dbzero object from serialized bytes, withing the snapshot context.

        Parameters
        ----------
        data : bytes
            The bytes object previously created by dbzero.serialize().

        Returns
        -------
        Any
            A dbzero object that was encoded in the data bytes.
        """
        ...

    def close(self) -> None:
        """Close dbzero snapshot."""
        ...

    def get_state_num(self) -> int:
        """Get state number of a snapshot.

        Returns
        -------
        int
            State number of a snapshot.
        """
        ...