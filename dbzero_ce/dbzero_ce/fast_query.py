# This is an experimental version of a possible Query Engine
# implementation for DBZero

import dbzero_ce as db0
from typing import Dict


@db0.memo(singleton=True)
class FastQueryCache:
    def __init__(self):
        # query UUID / (state number, last result)
        self.__last_results = {}
    
    def get_last_result(self, query):
        """
        Retrieves the latest known snapshot storing query result
        """        
        return self.__last_results.get(db0.uuid(query), None)

    def update(self, query, result):
        """
        Updates the cache with the latest query result
        """
        self.__last_results[db0.uuid(query)] = (db0.get_state_num(), result)
    

@db0.memo
class GroupByBucket:
    def __init__(self):
        self.__count = 0
    
    def collect(self, row):
        self.__count += 1
    
    def count(self):
        return self.__count
    

class GroupByEval:
    def __init__(self, key_func, data = None):
        self.__data = data if data is not None else {}
        self.__key_func = key_func
    
    def add(self, query):
        for row in query:
            key = self.__key_func(row)
            bucket = self.__data.get(key, None)
            if bucket is None:
                bucket = GroupByBucket()
                self.__data[key] = bucket
            bucket.collect(row)
        
    def collect(self):
        result = self.__data
        self.__data = {}
        return result        
    
    
def group_by(key_func, query) -> Dict:
    """
    Group by the query results by the given key
    """
    cache = FastQueryCache()
    last_result = cache.get_last_result(query)
    query_eval = GroupByEval(key_func, last_result[1] if last_result is not None else None)    
    if last_result is None:
        # evaluate full query
        query_eval.add(query)
    else:
        # evaluate deltas
        snap_1 = db0.snapshot(last_result[0])        
        query_1 = snap_1.deserialize(db0.serialize(query))        
        # insertions since last result
        query_eval.add(db0.find(query, db0.no(query_1)))
        # FIXME: handle deletions

    result = query_eval.collect()
    cache.update(query, result)
    return result