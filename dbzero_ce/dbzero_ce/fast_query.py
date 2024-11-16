# This is an experimental version of a possible Query Engine
# implementation for DBZero
import dbzero_ce as db0
from typing import Any, Dict


__px_fast_query = None

def init_fast_query(prefix=None):
    global __px_fast_query
    if prefix:
        __px_fast_query = prefix
    # FQ prefix must be available for read/write
    if __px_fast_query:
        db0.open(__px_fast_query, "rw")
    

class FastQuery:
    def __init__(self, query, group_defs=None, uuid=None, sig=None, bytes=None, snapshot=None):
        self.__query = query
        # split query by all available group definitions
        for group_def in group_defs:
            if group_def.groups is not None:
                self.__query = db0.split_by(group_def.groups, self.__query)
        self.__group_defs = group_defs
        self.__uuid= uuid
        self.__sig = sig
        self.__bytes = bytes
        self.__snapshot = snapshot
    
    # rebase to a (different) snapshot
    def rebase(self, snapshot):
        return FastQuery(snapshot.deserialize(self.bytes), self.__group_defs, self.__uuid, 
            self.__sig, self.bytes, snapshot)
    
    @property
    def snapshot(self):
        if self.__snapshot is None:
            raise ValueError("FastQuery snapshot not set")
        return self.__snapshot
    
    @property
    def uuid(self):
        if self.__uuid is None:
            self.__uuid = db0.uuid(self.__query)
        return self.__uuid
    
    @property
    def signature(self):
        if self.__sig is None:
            self.__sig = self.__query.signature()
        return self.__sig

    @property
    def rows(self):
        return self.__query
    
    def compare(self, bytes):
        # compare this query to bytes (serialized query)
        return self.__query.compare(db0.deserialize(bytes))

    @property
    def bytes(self):
        if self.__bytes is None:
            self.__bytes = db0.serialize(self.__query)
        return self.__bytes


class GroupDef:
    def __init__(self, key_func=None, groups=None):
        self.__key_func = key_func
        # extract decorators as the group identifier (may be one or more)
        self.key_func = key_func if key_func else lambda row: row[1:][0]
        self.groups = groups
    
    def split(self):
        # prepare key func to work with split rows
        if self.__key_func is not None:
            self.key_func = lambda row: self.__key_func(row[0])            
    
    def __call__(self, row) -> Any:
        return self.key_func(row)
    
    
@db0.memo(singleton=True)
class FastQueryCache:
    def __init__(self, prefix=None):
        db0.set_prefix(self, prefix)
        # queries by signature
        self.__cache = {}
    
    def find_result(self, query: FastQuery):
        """
        Retrieves the latest known snapshot storing query result
        """
        # identify queries with the same signature first
        results = self.__cache.get(query.signature, None)
        if results is None:
            return None

        # if the result with an identical uuid is found, return it
        if query.uuid in results:
            return results[query.uuid]
        
        # from the remaining results, pick the closest one
        min_diff = 1.0
        min_result = None
        for cached_result in results.values():
            diff = query.compare(cached_result[1])
            if diff < min_diff:
                min_diff = diff
                min_result = cached_result
        
        # return result of the closest query on condition it's sufficiently close        
        return min_result if min_diff < 0.33 else None
    
    def update(self, state_num, query: FastQuery, result):
        """
        Updates the cache with results computed over a more recent state number
        NOTE: state_num must be a finalized transaction number
        """
        if query.signature not in self.__cache:
            self.__cache[query.signature] = {}

        # key = a tuple of: state number, serialized query bytes, full query result
        self.__cache[query.signature][query.uuid] = (state_num, query.bytes, result)                
        return (state_num, query.bytes, result)
    
    def get_cache_keys(self):
        return list(self.__cache.keys())
    
    
def count_op(status, row_groups=None):
    """
    Update (or initialize status) with rows to remove (0) or rows to add (1)
    """
    if row_groups is None:
        return 0
    else:
        status -= len(row_groups[0]) if row_groups[0] is not None else 0
        status += len(row_groups[1]) if row_groups[1] is not None else 0
    return status
    
    
def make_sum(value_func):
    """
    Generates sum-op with a specific value function
    """
    def sum_op(status, row_groups=None):
        """
        Update (or initialize status) with rows to remove (0) or rows to add (1)
        """
        if row_groups is None:
            return 0
        else:
            status -= sum(value_func(row[0]) for row in row_groups[0]) if row_groups[0] is not None else 0
            status += sum(value_func(row[0]) for row in row_groups[1]) if row_groups[1] is not None else 0
        return status
    
    return sum_op

    
@db0.memo
class GroupByBucket:
    def __init__(self, ops=(count_op,), prefix=None):
        # FIXME: ops should be cached in Python when this feature is available
        db0.set_prefix(self, prefix)
        self.__status = tuple([op(None) for op in ops])
    
    def update(self, row_groups, ops=(count_op,)):
        self.__status = tuple([op(status, row_groups) for op, status in zip(ops, self.__status)])
    
    @property
    def result(self):
        if len(self.__status) == 1:
            return self.__status[0]
        # return as a Python native type
        return db0.load(self.__status)
        
    
class GroupByEval:
    def __init__(self, group_defs, data=None, prefix=None):
        self.__data = data if data is not None else {}
        self.__prefix = prefix
        if len(group_defs) == 1:
            group_def = group_defs[0]
            self.__group_builder = lambda row: group_def(row)
        else:
            self.__group_builder = lambda row: tuple(group_def(row) for group_def in group_defs)

    def update(self, row_groups, ops, max_scan=None):
        __groups = {}
        for side_num in range(2):
            if row_groups[side_num] is None:
                continue
            for row in row_groups[side_num]:
                if max_scan is not None:
                    if max_scan == 0:
                        raise MaxScanExceeded()
                    max_scan -= 1
                key = self.__group_builder(row)
                row_lists = __groups.get(key, None)
                if row_lists is None:
                    row_lists = ([], [])
                    __groups[key] = row_lists
                row_lists[side_num].append(row)
        
        # now feed groups into buckets
        for key, row_lists in __groups.items():
            bucket = self.__data.get(key, None)
            if bucket is None:
                bucket = GroupByBucket(ops, prefix=self.__prefix)
                self.__data[key] = bucket
            bucket.update(row_lists, ops)
    
    def release(self):
        result = self.__data
        self.__data = {}
        return result


class MaxScanExceeded(Exception):
    pass
    
    
def group_by(group_defs, query, ops=(count_op,), max_scan=1000) -> Dict:
    """
    Group query results by the given key
    """
    def delta(start, end):
        # compute delta between the 2 snapshots                
        snap = end.snapshot
        # use the "end" snapshot for rows retrieval
        return snap.find(end.rows, db0.no(start.rows))
    
    px_name = db0.get_prefix_of(query).name
    # a simple group definition is either: a string, a lambda or iterable of strings/enum values
    def is_simple_group_def(group_defs):
        if isinstance(group_defs, str) or callable(group_defs):
            return True
        if hasattr(group_defs, "__iter__"):
            return all(isinstance(item, str) or db0.is_enum_value(item) for item in group_defs)
        return False

    # extract groups and key function from a simple group definition
    def prepare_group_defs(group_defs, inner_def = False):
        if is_simple_group_def(group_defs):
            if hasattr(group_defs, "__iter__"):
                yield GroupDef(groups = group_defs)
            else:
                yield GroupDef(key_func = group_defs)
        else:
            if inner_def:
                raise ValueError("Invalid group definition")
            yield from (next(prepare_group_defs(item, True)) for item in group_defs)
    
    group_defs = tuple(prepare_group_defs(group_defs))
    if any(group_def.groups is not None for group_def in group_defs):
        for grop_def in group_defs:
            grop_def.split()
    
    def try_query_eval(fast_query, last_result, ops, max_scan):
        query_eval = GroupByEval(group_defs, last_result[2] if last_result is not None else None, prefix=__px_fast_query)
        if last_result is None:
            # no cached result, evaluate full query
            query_eval.update((None, fast_query.rows), ops, max_scan)
        else:
            # evaluate from deltas
            old_query = fast_query.rebase(db0.snapshot({px_name: last_result[0]}))
            # row groups = (rows removed since last update, rows added since last update) as delta iterators
            query_eval.update((delta(old_query, fast_query), delta(fast_query, old_query)), ops, max_scan)
        return query_eval.release()
    
    def format_result(result):
        return {db0.load(key): value.result for key, value in result.items()}
    
    cache = FastQueryCache(prefix=__px_fast_query)
    # take snapshot of the latest known state and rebase input query
    # otherwise refresh might invalidate query results (InvalidStateError)
    with db0.snapshot() as snapshot:
        fast_query = FastQuery(query, group_defs).rebase(snapshot)
        last_result = cache.find_result(fast_query)
        
        # do not limit max_scan when on a first transaction
        state_num = snapshot.get_state_num(prefix=px_name)
        if state_num <= 1:
            max_scan = None

        # return the cached result if from the same state number
        if last_result is None or last_result[0] != state_num:
            try:
                result = try_query_eval(fast_query, last_result, ops, max_scan)
            except MaxScanExceeded:
                # go back to the last finalized transaction and compute the result (possibly using deltas)                
                result = try_query_eval(fast_query, last_result, ops, max_scan=None)
            # update the cache with the result
            last_result = cache.update(state_num, fast_query, result)
        
        # return result from cache (formatted)
        return format_result(last_result[2])
    