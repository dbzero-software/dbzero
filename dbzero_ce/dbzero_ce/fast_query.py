# This is an experimental version of a possible Query Engine
# implementation for DBZero
import dbzero_ce as db0
from typing import Dict


class FastQuery:
    def __init__(self, query, groups = None, uuid = None, sig = None, bytes = None):        
        self.__query = query if groups is None else db0.split_by(groups, query)
        self.__groups = groups
        self.__uuid= uuid
        self.__sig = sig        
        self.__bytes = bytes
    
    # rebase to a different snapshot
    def rebase(self, snapshot):
        return FastQuery(snapshot.deserialize(db0.serialize(self.__query)), self.__groups, 
                         self.__uuid, self.__sig, self.__bytes)
    
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
    

@db0.memo(singleton=True)
class FastQueryCache:
    def __init__(self):
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
            # FIXME: change to tuple unpack when fixed by Adrian
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
        
        results = self.__cache[query.signature]
        # key = a tuple of: state number, serialized query bytes, full query result
        results[query.uuid] = (state_num, query.bytes, result)
        return (state_num, query.bytes, result)
    

@db0.memo
class GroupByBucket:
    def __init__(self):
        self.__count = 0
    
    def add(self, row):
        self.__count += 1
    
    def remove(self, row):
        self.__count -= 1
    
    def count(self):
        return self.__count
    
    
class GroupByEval:
    def __init__(self, key_func, data = None):
        self.__data = data if data is not None else {}
        if key_func:
            self.__key_func = key_func
        else:
            # extract decorators as the group identifier (may be one or more)
            self.__key_func = lambda row: row[1:][0]
    
    def add(self, rows, max_scan = None):
        for row in rows:
            if max_scan is not None:
                if max_scan == 0:
                    raise MaxScanExceeded()
                max_scan -= 1
            key = self.__key_func(row)
            bucket = self.__data.get(key, None)
            if bucket is None:
                bucket = GroupByBucket()
                self.__data[key] = bucket
            bucket.add(row)
        return max_scan
    
    def remove(self, rows, max_scan = None):
        for row in rows:
            if max_scan is not None:
                if max_scan == 0:
                    raise MaxScanExceeded()
                max_scan -= 1            
            key = self.__key_func(row)
            self.__data.get(key).remove(row)
        return max_scan

    def release(self):
        result = self.__data
        self.__data = {}
        return result


class MaxScanExceeded(Exception):
    pass
    
    
def group_by(group_defs, query, max_scan = 1000) -> Dict:
    """
    Group query results by the given key
    """
    def delta(start, end):
        return db0.find(end.rows, db0.no(start.rows))
    
    # extract groups and key function
    def prepare_group_defs(group_defs):
        # if iterable        
        return (None, group_defs) if hasattr(group_defs, "__iter__") else (group_defs, None)
    
    key_func, groups = prepare_group_defs(group_defs)
    cache = FastQueryCache()
    def try_query_eval(fast_query, last_result, max_scan):
        query_eval = GroupByEval(key_func, last_result[2] if last_result is not None else None)
        if last_result is None:
            # no cached result, evaluate full query
            query_eval.add(fast_query.rows, max_scan)
        else:
            # evaluate from deltas
            old_query = fast_query.rebase(db0.snapshot(last_result[0]))
            # insertions since last result
            limit = query_eval.add(delta(old_query, fast_query), max_scan)
            query_eval.remove(delta(fast_query, old_query), limit)
        return query_eval.release()
    
    last_result = cache.find_result(query)
    fast_query = FastQuery(query, groups)
    # do not limit max_scan when on a first transaction
    max_scan = max_scan if db0.get_state_num() > 1 else None
    while True:
        try:
            return try_query_eval(fast_query, last_result, max_scan)
        except MaxScanExceeded:
            max_scan = None
            # go back to the last finalized transaction and compute the result (possibly using deltas)
            # FIXME: change to db0.get_state_num(finalized = True) when feature available
            state_num = db0.get_state_num() - 1
            result = try_query_eval(fast_query.rebase(db0.snapshot(state_num)), last_result, max_scan)
            # update the cache with the result
            last_result = cache.update(state_num, fast_query, result)
