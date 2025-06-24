import dbzero_ce as db0
from .dbzero_ce import _select_mod_candidates, _split_by_snapshots


def select_new(query, pre_snapshot, last_snapshot):
    """
    Refines the query to include only new objects in the given state range (scope)
    :param query: the query to refine
    :param context: context object which needs to be managed by the calling function    
    """
    # there's no initial state, therefore all results will be "new"
    if not pre_snapshot:
        return query
    
    query_data = db0.serialize(query)
    # combine the pre- and post- queries
    return last_snapshot.find(
        last_snapshot.deserialize(query_data), db0.no(pre_snapshot.deserialize(query_data))
    )
    
    
def select_deleted(query, pre_snapshot, last_snapshot):
    """
    Refines the query to include only objects which were deleted within the given state range (scope)
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
    

def select_modified(query, pre_snapshot, last_snapshot, compare_with = None):
    """
    Refines the query to include only objects which were modified within the given state range (scope)
    not including new objects
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
    