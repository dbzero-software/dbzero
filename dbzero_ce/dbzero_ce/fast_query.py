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
            cached_result = results[query.uuid]
            return (cached_result[0], cached_result[2])
        
        # from the remaining results, pick the closest one
        min_diff = 1.0
        min_result = None
        for cached_result in results.values():
            # FIXME: change to tuple unpack when fixed by Adrian
            diff = query.compare(cached_result[1])
            if diff < min_diff:
                min_diff = diff
                min_result = (cached_result[0], cached_result[2])
        
        # return result of the closest query on condition it's sufficiently close
        return min_result if min_diff < 0.33 else None
    
    def update(self, query: FastQuery, result):
        """
        Updates the cache with the latest query result
        """
        if query.signature not in self.__cache:
            self.__cache[query.signature] = {}
        
        results = self.__cache[query.signature]
        results[query.uuid] = (db0.get_state_num(), query.bytes, result)
    

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
    def __init__(self, key_func, groups, data = None):
        self.__data = data if data is not None else {}
        if key_func:
            self.__key_func = key_func
        else:
            # extract all decorators as the group identifier
            self.__key_func = lambda row: row[1:]
    
    def add(self, rows):
        for row in rows:
            key = self.__key_func(row)
            bucket = self.__data.get(key, None)
            if bucket is None:
                bucket = GroupByBucket()
                self.__data[key] = bucket
            bucket.add(row)
    
    def remove(self, rows):
        for row in rows:
            key = self.__key_func(row)
            self.__data.get(key).remove(row)

    def release(self):
        result = self.__data
        self.__data = {}
        return result
    
    
def group_by(group_defs, query) -> Dict:
    """
    Group by the query results by the given key
    """
    def delta(start, end):
        return db0.find(end.rows, db0.no(start.rows))
    
    # extract groups and key function
    def prepare_group_defs(group_defs):
        # if iterable        
        return (None, group_defs) if hasattr(group_defs, "__iter__") else (group_defs, None)

    key_func, groups = prepare_group_defs(group_defs)
    cache = FastQueryCache()
    query = FastQuery(query, groups)
    last_result = cache.find_result(query)
    query_eval = GroupByEval(key_func, groups, last_result[1] if last_result is not None else None)
    if last_result is None:
        # evaluate full query
        query_eval.add(query.rows)
    else:
        # evaluate deltas
        old_query = query.rebase(db0.snapshot(last_result[0]))
        # insertions since last result
        query_eval.add(delta(old_query, query))
        query_eval.remove(delta(query, old_query))

    result = query_eval.release()
    cache.update(query, result)
    return result
