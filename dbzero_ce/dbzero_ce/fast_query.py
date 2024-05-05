# This is an experimental version of a possible Query Engine
# implementation for DBZero

import dbzero_ce as db0
from typing import Dict


@db0.memo(singleton=True)
class FastQueryCache:
    def __init__(self):
        # query UUID / state number
        self.__last_results = {}
    
    def get_last_result(self, query):
        """
        Retrieves the latest known snapshot storing query result
        """
        return db0.snapshot(self.__last_state_num, self.__last_result)
    
    def update(self, query, result):
        print("Updating cache")
        """
        Updates the cache with the latest query result
        """
        self.__last_state_num = db0.get_state_num()
        print("Updating result")
        self.__last_result = result
    

@db0.memo    
class GroupByBucket:
    def __init__(self):
        self.__count = 0
    
    def collect(self, row):
        self.__count += 1
    
    
def group_by(query) -> Dict:
    """
    Group by the query results by the given key
    """
    print("Inside group by")
    cache = FastQueryCache()
    print("Fast query cache created")
    # last_result = cache.get_last_result(query)
    # if last_result is not None:
    #     # execute as a delta
    #     print("Delta query !!!")
    # else:
    #     result = {}
    #     for row in query:
    #         bucket = result.get(row.key, None)
    #         if bucket is None:
    #             bucket = GroupByBucket()
    #             result[row.key] = bucket
    
    # cache.update(query, result)
    # return result
