import dbzero_ce as db0
from .dbzero_ce import _select_mod_candidates


def __prepare_context(context, query, from_state: int, to_state: int = None):
    px_name = db0.get_prefix_of(query).name
    if not to_state:
        to_state = db0.get_state_num(prefix = px_name, finalized = True)
    
    context.px_name = px_name
    context.from_state = from_state
    context.to_state = to_state
    context.pre_snap = db0.snapshot({px_name: from_state - 1})
    context.post_snap = db0.snapshot({px_name: to_state})


def select_new(query, context, from_state: int, to_state: int = None):
    """
    Refines the query to include only new objects in the given state range (scope)
    :param query: the query to refine
    :param context: context object which needs to be managed by the calling function    
    """
    # there's no state before 1, therefore all results will be "new"
    if from_state <= 1:
        return query

    __prepare_context(context, query, from_state, to_state)
    query_data = db0.serialize(query)
    # combine the pre- and post- queries
    return context.post_snap.find(
        context.post_snap.deserialize(query_data), db0.no(context.pre_snap.deserialize(query_data))
    )
    
    
def select_deleted(query, context, from_state: int, to_state: int = None):
    """
    Refines the query to include only objects which were deleted within the given state range (scope)
    """
    # there's no state before 1, so no pre-existing objects could've been deleted
    if from_state <= 1:
        return []

    __prepare_context(context, query, from_state, to_state)    
    query_data = db0.serialize(query)
    return context.pre_snap.find(
        context.pre_snap.deserialize(query_data), db0.no(context.post_snap.deserialize(query_data))
    )


def select_modified(query, context, from_state: int, to_state: int = None, compare_with = None):
    """
    Refines the query to include only objects which were modified within the given state range (scope)
    not including new objects
    """
    # there's no state before 1, so no pre-existing objects could've been modified
    if from_state <= 1:
        return []

    __prepare_context(context, query, from_state, to_state)
    query_data = db0.serialize(query)
    pre_query = context.pre_snap.deserialize(query_data)
    
    post_query = context.post_snap.deserialize(query_data)
    post_mod = _select_mod_candidates(post_query, (from_state, to_state))
    
    # NOTE: created objects are not reported (only the ones existing in the pre-snapshot)
    # FIXME: must compare objects to identify modified
    return context.post_snap.find(post_mod, pre_query)
