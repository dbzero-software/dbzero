# This is an experimental version of a possible Query Engine
# implementation for DBZero

import dbzero_ce as db0
from typing import Dict


class FastQuery:
    def __init__(self, query, uuid = None):
        self.__query = query
        self.__uuid = uuid if uuid is not None else db0.uuid(query.as_runnable())
    
    # rebase to a different snapshot
    def rebase(self, snapshot):
        return FastQuery(snapshot.deserialize(db0.serialize(self.__query)), self.__uuid)

    @property
    def uuid(self):
        return self.__uuid
    
    @property
    def rows(self):
        return self.__query


@db0.memo(singleton=True)
class FastQueryCache:
    def __init__(self):
        # query UUID / (state number, last result)
        self.__last_results = {}
    
    def get_last_result(self, query: FastQuery):
        """
        Retrieves the latest known snapshot storing query result
        """        
        return self.__last_results.get(query.uuid, None)

    def update(self, query: FastQuery, result):
        """
        Updates the cache with the latest query result
        """
        self.__last_results[query.uuid] = (db0.get_state_num(), result)
    

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
        self.__key_func = key_func
    
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

    def collect(self):
        result = self.__data
        self.__data = {}
        return result
    
    
def group_by(key_func, query) -> Dict:
    """
    Group by the query results by the given key
    """
    def diff(start, end):
        return db0.find(end.rows, db0.no(start.rows))
    
    cache = FastQueryCache()
    query = FastQuery(query)
    last_result = cache.get_last_result(query)
    query_eval = GroupByEval(key_func, last_result[1] if last_result is not None else None)
    if last_result is None:
        # evaluate full query
        query_eval.add(query.rows)
    else:
        # evaluate deltas
        old_query = query.rebase(db0.snapshot(last_result[0]))
        # insertions since last result
        query_eval.add(diff(old_query, query))
        query_eval.remove(diff(query, old_query))

    result = query_eval.collect()
    cache.update(query, result)
    return result