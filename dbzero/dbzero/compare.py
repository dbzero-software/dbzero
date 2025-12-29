# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from typing import Optional, List
from .interfaces import Memo, Tag
import dbzero as db0
from .dbzero import _compare


def compare(obj_1: Memo, obj_2: Memo, tags: Optional[List[Tag]] = None) -> bool:
    """Perform a deep, content-based comparison of two Memo objects to check if they are identical.

    By default, it only compares the objects' data fields.
    When optional `tags` parameter is provided, their tag assignments are included in the comparison.

    Parameters
    ----------
    obj_1 : Memo
        The first Memo object for comparison.
    obj_2 : Memo
        The second Memo object for comparison.
    tags : list of Tag, optional
        A list of tags to include in the comparison. If provided, the method 
        will check if both objects have an identical assignment (or lack) of each 
        tag in the list.

    Returns
    -------
    bool
        True if the objects are of the same type and their data content (and 
        specified tags, if checked) are identical. False if the objects are of 
        different types, their data differs, or their specified tag assignments differ.

    Examples
    --------
    Basic content comparison:

    >>> obj_1 = MemoTestClass(100)
    >>> obj_2 = MemoTestClass(100)
    >>> dbzero.compare(obj_1, obj_2)  # Returns True
    >>> 
    >>> # Change the content of one object
    >>> obj_2.value = 200
    >>> dbzero.compare(obj_1, obj_2)  # Returns False

    Comparing with tags:
    
    >>> obj_A = MemoTestClass(100)
    >>> obj_B = MemoTestClass(100)
    >>> 
    >>> dbzero.tags(obj_B).add("featured")
    >>> dbzero.commit()
    >>> 
    >>> # Default comparison ignores tags and returns True
    >>> # because their content is the same.
    >>> assert dbzero.compare(obj_A, obj_B) is True
    >>> 
    >>> # Including the 'featured' tag in the comparison
    >>> # returns False because obj_A lacks the tag.
    >>> assert dbzero.compare(obj_A, obj_B, tags=['featured']) is False
    >>> 
    >>> # Now add the tag to obj_A as well
    >>> dbzero.tags(obj_A).add("featured")
    >>> dbzero.commit()
    >>> 
    >>> # The comparison now returns True
    >>> assert dbzero.compare(obj_A, obj_B, tags=['featured']) is True
    """
    if _compare(obj_1, obj_2):
        # if objects are identical then also compare tags
        if tags is not None:
            # retrieve associated snapshots since objects may come from different ones
            snap_1 = db0.get_snapshot_of(obj_1)
            snap_2 = db0.get_snapshot_of(obj_2)
            for tag in tags:
                if len(snap_1.find(tag, obj_1)) != len(snap_2.find(tag, obj_2)):
                    return False
        # tags are identical
        return True
                
    return False