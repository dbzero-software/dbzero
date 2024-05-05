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
    
    
def group_by(key_func, query) -> Dict:
    """
    Group by the query results by the given key
    """    
    print(f"DB0 state num: {db0.get_state_num()}")
    print(f"Query UUID: {db0.uuid(query)}")
    cache = FastQueryCache()
    last_result = cache.get_last_result(query)
    if last_result is not None:
        # execute as a delta
        print("Delta query !!!")
        raise NotImplementedError()
    else:
        result = {}
        for row in query:
            key = key_func(row)
            bucket = result.get(key, None)
            if bucket is None:
                bucket = GroupByBucket()
                result[key] = bucket
            bucket.collect(row)
    
    cache.update(query, result)
    return result
