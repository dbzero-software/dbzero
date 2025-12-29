# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from typing import Optional, Callable, Iterable, Tuple
from .interfaces import Memo, QueryObject, Snapshot
import dbzero as db0
from .dbzero import _select_mod_candidates, _split_by_snapshots


def select_new(query: QueryObject, pre_snapshot: Optional[Snapshot], last_snapshot: Snapshot) -> QueryObject:
    """Refine the query to only include objects that were created between two snapshots.

    Objects that match the query condition in later snapshot and do not match in earlier
    snapshot are considered 'new' and are included in the result set.

    Parameters
    ----------
    query : QueryObject
        A dbzero query.
    pre_snapshot : Snapshot or None
        The snapshot representing the starting point for the comparison. If None or doesn't 
        contain the prefix query, all objects matching the query found in last_snapshot will
        be returned as new.
    last_snapshot : Snapshot
        The snapshot representing the ending point for the comparison.

    Returns
    -------
    QueryObject
        A new query containing the objects that match the input query and were created 
        between pre_snapshot and last_snapshot.

    Examples
    --------
    Find newly created objects:
    
    >>> # Create an initial state and snapshot
    >>> dbzero.commit()
    >>> snap_1 = dbzero.snapshot()
    >>> 
    >>> # Create a new District object and commit the change
    >>> new_district = District("some_District")
    >>> dbzero.commit()
    >>> snap_2 = dbzero.snapshot()
    >>> 
    >>> # Query for new District objects created between snap_1 and snap_2
    >>> new_objects = dbzero.select_new(dbzero.find(District), snap_1, snap_2)
    >>> 
    >>> assert len(new_objects) == 1
    >>> assert next(iter(new_objects)) == new_district
    """
    # there's no initial state, therefore all results from the last_snapshot will be "new"
    query_data = db0.serialize(query)
    if pre_snapshot is None or db0.get_prefix_of(query).name not in pre_snapshot:
        return last_snapshot.deserialize(query_data)
    
    # combine the pre- and post- queries
    return last_snapshot.find(
        last_snapshot.deserialize(query_data), db0.no(pre_snapshot.deserialize(query_data))
    )
    
    
def select_deleted(query: QueryObject, pre_snapshot: Optional[Snapshot], last_snapshot: Snapshot) -> QueryObject:
    """Refine the query to only include objects that were deleted between two snapshots.

    Objects that match the query condition in earlier snapshot and do not match in later
    snapshot are considered 'deleted' and are included in the result set.

    Parameters
    ----------
    query : QueryObject
        A dbzero query.
    pre_snapshot : Snapshot or None
        The snapshot representing the starting point for the comparison.
        If None, returns an empty list since there's no initial state to compare against.
    last_snapshot : Snapshot
        The snapshot representing the ending point for the comparison.

    Returns
    -------
    QueryObject
        A new query containing the objects that match the input query and were deleted 
        between pre_snapshot and last_snapshot.

    Examples
    --------
    Find objects that were untagged between snapshots:
    
    >>> @dbzero.memo
    ... class Task:
    ...     def __init__(self, name):
    ...         self.name = name
    >>> 
    >>> # Create two tasks and tag them as 'active'
    >>> task1 = Task("Write docs")
    >>> dbzero.tags(task1).add("active")
    >>> task2 = Task("Review code")
    >>> dbzero.tags(task2).add("active")
    >>> dbzero.commit()
    >>> 
    >>> # Capture the "before" snapshot
    >>> snap_before = dbzero.snapshot()
    >>> 
    >>> # "Delete" task1 from the 'active' view by removing its tag
    >>> dbzero.tags(task1).remove("active")
    >>> dbzero.commit()
    >>> 
    >>> # Capture the "after" snapshot
    >>> snap_after = dbzero.snapshot()
    >>> 
    >>> # Find which tasks were removed from the query results
    >>> active_tasks_query = dbzero.find(Task, "active")
    >>> deleted_tasks = dbzero.select_deleted(active_tasks_query, snap_before, snap_after)
    >>> 
    >>> assert len(deleted_tasks) == 1
    >>> assert deleted_tasks[0].name == "Write docs"
    """
    # there's no initiali state, so no pre-existing objects could've been deleted
    if not pre_snapshot:
        return []
    
    query_data = db0.serialize(query)
    return pre_snapshot.find(
        pre_snapshot.deserialize(query_data), db0.no(last_snapshot.deserialize(query_data))
    )


class ModIterator:
    def __init__(self, query, compare_with):
        self.__query = query
        self.__compare_with = compare_with
        self.__iter = iter(self.__query)
    
    def __iter__(self):
        return ModIterator(self.__query, self.__compare_with)

    def __next__(self):
        while True:
            obj_1, obj_2 = next(self.__iter)
            if not self.__compare_with(obj_1, obj_2):
                return obj_1, obj_2


class ModIterable:
    def __init__(self, query, compare_with):
        self.__query = query
        self.__compare_with = compare_with
    
    def __iter__(self):
        return ModIterator(self.__query, self.__compare_with)
    
    def __len__(self):
        size = 0
        my_iter = iter(self)
        while True:
            try:
                next(my_iter)
                size += 1
            except StopIteration:
                break
        return size
    

def select_modified(query: QueryObject, pre_snapshot: Optional[Snapshot], last_snapshot: Snapshot, compare_with: Optional[Callable] = None) -> Iterable[Tuple[Memo, Memo]]:
    """Refines the query to include only objects which were modified between two snapshots,
    not including new objects.

    Objects that match the query condition in both snapshots and their state was modified
    between two snapshots are included in the result set.

    Parameters
    ----------
    query : QueryObject
        A dbzero query.
    pre_snapshot : Snapshot or None
       The snapshot representing the starting point for the comparison.
        If None, returns an empty list since there's no initial state to compare against.
    last_snapshot : Snapshot
        The snapshot representing the ending point for the comparison.
    compare_with : callable, optional
        A custom comparator that takes two arguments: the object from pre_snapshot and the object 
        from last_snapshot. It should return True if the objects are considered unchanged.

    Returns
    -------
    Iterable[Tuple[Memo, Memo]]
        An iterable of tuples, where each tuple contains the "before" and "after" versions 
        of a modified object: (old_version, new_version). old_version is the object's state 
        from pre_snapshot, new_version is the object's state from last_snapshot.

    Examples
    --------
    Basic usage - find all modified districts:
    
    >>> # Initial state
    >>> district_a = District(name="Oldtown")
    >>> dbzero.commit()
    >>> snap_1 = dbzero.snapshot()
    >>> 
    >>> # Make a change
    >>> district_a.name = "Newtown"
    >>> dbzero.commit()
    >>> snap_2 = dbzero.snapshot()
    >>> 
    >>> # Find modified objects
    >>> modified_districts = dbzero.select_modified(dbzero.find(District), snap_1, snap_2)
    >>> 
    >>> for old_ver, new_ver in modified_districts:
    ...     print(f"District name changed from '{old_ver.name}' to '{new_ver.name}'")
    ...     assert old_ver.name == "Oldtown"
    ...     assert new_ver.name == "Newtown"

    Advanced usage - custom comparison logic:
    
    >>> class Product:
    ...     def __init__(self, name, price, last_updated):
    ...         self.name = name
    ...         self.price = price
    ...         self.last_updated = last_updated
    >>> 
    >>> # Initial state
    >>> product = Product(name="Laptop", price=1200, last_updated=1672531200)
    >>> dbzero.commit()
    >>> snap_1 = dbzero.snapshot()
    >>> 
    >>> # Modify only the timestamp
    >>> product.last_updated = 1672617600
    >>> dbzero.commit()
    >>> snap_2 = dbzero.snapshot()
    >>> 
    >>> # Custom comparison function
    >>> def a_meaningful_change(obj1, obj2):
    ...     # Return True if only the timestamp is different (i.e., they are "equal" for our purposes)
    ...     return obj1.name == obj2.name and obj1.price == obj2.price
    >>> 
    >>> # This will find no "meaningful" modifications
    >>> results = dbzero.select_modified(dbzero.find(Product), snap_1, snap_2, compare_with=a_meaningful_change)
    >>> assert len(results) == 0
    """
    # there's no state before 1, so no pre-existing objects could've been modified
    if not pre_snapshot:
        return []
    
    query_data = db0.serialize(query)
    pre_query = pre_snapshot.deserialize(query_data)
    
    post_query = last_snapshot.deserialize(query_data)
    px_name = db0.get_prefix_of(query).name
    post_mod = _select_mod_candidates(
        post_query, (pre_snapshot.get_state_num(px_name) + 1, last_snapshot.get_state_num(px_name)))
    
    # NOTE: created objects are not reported (only the ones existing in the pre-snapshot)
    query = last_snapshot.find(post_mod, pre_query)
    if compare_with:
        # NOTE: _split_by_snapshots returns tuples from both pre- and post-snapshots
        return ModIterable(_split_by_snapshots(query, pre_snapshot, last_snapshot), compare_with)
    
    return _split_by_snapshots(query, pre_snapshot, last_snapshot)
    