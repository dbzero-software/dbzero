# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

# This is an experimental version of a possible Query Engine
# implementation for dbzero
from typing import Union, Callable, Tuple, Dict
from .interfaces import QueryObject, Tag
import dbzero as db0
import inspect
import typing
import types as py_types
import re


__px_fast_query = None
__lambda_regex = re.compile(r'lambda\s.*?:\s*([^,)]*)')


def init_fast_query(prefix: str) -> None:
    """Initialize the fast query caching system using a specified prefix.

    This function designates a specific prefix to act as a cache for computationally 
    intensive queries, most notably `dbzero.group_by`. Useful when primary data prefix
    is accessed in a read-only mode, so the cache prefix can remain writable.

    Parameters
    ----------
    prefix : str
        The name of the prefix to use for caching. This prefix must be opened in
        read-write ("rw") mode.
    """
    global __px_fast_query
    if prefix:
        __px_fast_query = prefix
    # FQ prefix must be available for read/write
    if __px_fast_query:
        db0.open(__px_fast_query, "rw")
    

def get_lambda_source(func):
    global __lambda_regex
    assert isinstance(func, py_types.LambdaType)
    source_lines, _ = inspect.getsourcelines(func)
    source_code = ''.join(source_lines).strip()
    match = __lambda_regex.search(source_code)
    if match:
        return match.group(1).strip()
    else:
        raise ValueError("Could not extract lambda function implementation")


def signature_of(group_defs):
    if group_defs is None:
        return ""
    
    if hasattr(group_defs, "signature"):
        return group_defs.signature
    
    if isinstance(group_defs, py_types.LambdaType):
        sig_callable = inspect.signature(group_defs)
        return f"{len(sig_callable.parameters)}:{get_lambda_source(group_defs)}"

    if callable(group_defs):
        sig_callable = inspect.signature(group_defs)
        return f"{len(sig_callable.parameters)}:{inspect.getsource(group_defs)}"
    
    if hasattr(group_defs, "__iter__"):
        result = ""
        for group_def in group_defs:
            result += f"({signature_of(group_def)})"
        return result

    return repr(group_defs)

    
class FastQuery:
    def __init__(self, query, group_defs=None, uuid=None, sig=None, bytes=None, snapshot=None):
        self.__group_defs = group_defs
        self.__query, self.__is_simple = self.__make_query(query)
        self.__uuid= uuid
        self.__sig = sig
        self.__bytes = bytes
        self.__snapshot = snapshot
        self.__rows = self.__query
    
    def __make_query(self, query) -> typing.Tuple:
        result = query
        is_simple = True
        # split query by all available group definitions
        for group_def in self.__group_defs:
            if group_def.groups is not None:
                result = db0.split_by(group_def.groups, result)
                is_simple = False
        return (result, is_simple)
    
    # rebase to a (different) snapshot
    def rebase(self, snapshot):
        return FastQuery(snapshot.deserialize(self.bytes), self.__group_defs, self.__uuid, 
            self.__sig, self.bytes, snapshot)
    
    @property
    def is_simple(self):
        return self.__is_simple
    
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
            self.__sig = self.__query.signature() + signature_of(self.__group_defs)
        return self.__sig
    
    @property
    def rows(self):
        if self.__rows is None:
            # FIXME: replace with db0.iter when available
            if self.__snapshot is not None:
                rows = self.snapshot.deserialize(self.bytes)
            else:
                rows = db0.deserialize(self.bytes)
            self.__rows, self.__is_simple = self.__make_query(rows)
        
        result = self.__rows
        self.__rows = None
        return result
    
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
        # extract decorator as the group identifier
        self.key_func = key_func if key_func else lambda row: row[1]
        self.groups = groups
        self.__sig = None
    
    def split(self):
        # prepare key func to work with split rows
        if self.__key_func is not None:
            self.key_func = lambda row: self.__key_func(row[0])            
    
    @property
    def signature(self):
        if self.__sig is None:
            if self.groups is not None:
                self.__sig = signature_of(self.groups)
            else:
                self.__sig = signature_of(self.__key_func)
        return self.__sig
    
    def __call__(self, row) -> typing.Any:
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

        # value = a tuple of: state number, serialized query bytes, full query result
        self.__cache[query.signature][query.uuid] = (state_num, query.bytes, result)                
        return (state_num, query.bytes, result)
    
    def get_cache_keys(self):
        return list(self.__cache.keys())
    
    
def count_op(state, removed_rows, added_rows):
    """
    Update (or initialize state) with rows to remove (0) or rows to add (1)
    """
    if state is None:
        return 0
    else:
        state -= len(removed_rows)
        state += len(added_rows)
    return state
    
    
def make_sum(value_func):
    """
    Generates sum-op with a specific value function
    """
    def sum_op(state, removed_rows, added_rows):
        """
        Update (or initialize state) with rows to remove (0) or rows to add (1)
        """
        if state is None:
            return 0
        else:
            state -= sum(value_func(row) for row in removed_rows)
            state += sum(value_func(row) for row in added_rows)
        return state
    
    return sum_op

    
@db0.memo
class GroupByBucket:
    """
    Bucket associated with a single group key
    """
    def __init__(self, ops=(count_op,), prefix=None):
        # FIXME: ops should be cached in Python when this feature is available
        db0.set_prefix(self, prefix)
        # initialize ops (passing None to generate the initial state)
        self.__state = tuple([op(None, None, None) for op in ops])
        
    def update(self, removed_rows, added_rows, ops=(count_op,)):
        self.__state = tuple([op(state, removed_rows, added_rows) for op, state in zip(ops, self.__state)])
    
    @property
    def result(self):
        if len(self.__state) == 1:
            return self.__state[0]
        # return as a Python native type (i.e. load from dbzero)
        return db0.load(self.__state)
    
    
class GroupByEval:
    def __init__(self, group_defs, data=None, prefix=None):
        self.__data = data if data is not None else {}
        self.__prefix = prefix
        if len(group_defs) == 1:
            group_def = group_defs[0]
            # group_def is a callable - can directly be used as the group builder
            self.__group_builder = group_def
        else:
            # composite group builder
            self.__group_builder = lambda row: tuple(group_def(row) for group_def in group_defs)

    def update(self, row_groups, is_simple, ops):
        __groups = {}
        for side_num in range(2):
            if row_groups[side_num] is None:
                continue
            for row in row_groups[side_num]:
                key = self.__group_builder(row)
                row_lists = __groups.get(key, None)
                if row_lists is None:
                    row_lists = ([], [])
                    __groups[key] = row_lists
                row_lists[side_num].append(row if is_simple else row[0])

        # now, feed groups into buckets
        for key, row_lists in __groups.items():
            bucket = self.__data.get(key, None)
            if bucket is None:
                bucket = GroupByBucket(ops, prefix=self.__prefix)
                self.__data[key] = bucket
            bucket.update(row_lists[0], row_lists[1], ops)
    
    def release(self):
        result = self.__data
        self.__data = {}
        return result
    
    
def group_by(group_defs: Union[Callable, Tag, Tuple], query: QueryObject, ops: Tuple[Callable, ...] = (count_op,)) -> Dict:
    """Perform cached group-and-aggregate queries over a set of objects.

    The group_by() method categorizes objects returned by input query based on 
    one or more criteria and then applies aggregation operations to each category.

    The first time a specific query is run, it performs a full scan and caches the result.
    Subsequent group_by operations of the exact same query use cached result to speedup computation.

    Parameters
    ----------
    group_defs : lambda | Tag | tuple
        The criteria used to group the objects. This can be:
        
        * A lambda function: Applied to each object to determine its grouping key.
          For caching to work, the lambda's source code must be identical between calls.
        * Tag: To group objects by tags they are taged with.
          The group keys will be the string names of the enum members.
        * A tuple of the above: For multi-level grouping. The resulting dictionary keys 
          will be tuples.
    query : QueryObject
        A dbzero query to be grouped.
    ops : tuple of callable, default (count_op,)
        A tuple of aggregation operations to perform on each group.

    Returns
    -------
    dict
        A dictionary where:
        
        * Keys are the group identifiers determined by the group_defs criteria. If multiple 
          criteria are used, the key will be a tuple.
        * Values are the results of the aggregation(s). If a single operation is provided 
          in ops, the value is a single result (e.g., an int). If multiple operations are 
          provided, the value is a tuple containing the result of each operation in the 
          specified order.

    Examples
    --------
    Simple grouping by attribute:
    
    >>> # Assume objects are instances of a class with a 'key' attribute
    >>> objects = []
    >>> keys = ["one", "two", "three"]
    ... for i in range(10):
    ...     objects.append(SomeClass(key=keys[i % 3]))
    >>> dbzero.tags(*objects).add("my-tag")
    >>> 
    >>> # Group objects with "my-tag" by their 'key'
    >>> groups = dbzero.group_by(lambda row: row.key, dbzero.find("my-tag"))
    >>> # Example result: {'one': 4, 'two': 3, 'three': 3}

    Multi-level grouping:
    
    >>> Colors = dbzero.enum("Colors", ["RED", "GREEN", "BLUE"])
    >>> 
    >>> # Group by color tag and then by whether the value is even (0) or odd (1)
    >>> groups = dbzero.group_by(
    ...     (Colors.values(), lambda x: x.value % 2),
    ...     dbzero.find(MemoTestClass)
    ... )
    >>> # Example result: {('RED', 0): 2, ('RED', 1): 2, ('GREEN', 1): 3, ('BLUE', 0): 2, ...}

    Grouping with custom aggregations:
    
    >>> # Define two operations: default count and a sum of the 'value' attribute
    >>> query_ops = (dbzero.count_op, dbzero.make_sum(lambda x: x.value))
    >>> 
    >>> groups = dbzero.group_by(
    ...     lambda x: "even" if x.value % 2 == 0 else "odd",
    ...     dbzero.find(MemoTestClass),
    ...     ops=query_ops
    ... )
    >>> # Example result where each value is a tuple (count, sum_of_values):
    >>> # {'even': (5, 20), 'odd': (5, 25)}

    Notes
    -----
    This method creates and updates an internal cache to speed up subsequent identical queries.
    For the cache to be persistent across program runs, it must first be initialized using 
    dbzero.init_fast_query(). A query is considered "identical" if its parameters and its
    group_defs are the same as from the previous call
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
    
    def try_query_eval(fast_query, last_result, ops):
        query_eval = GroupByEval(group_defs, last_result[2] if last_result is not None else None, prefix=__px_fast_query)
        if last_result is None:
            # no cached result, evaluate full query
            query_eval.update((None, fast_query.rows), fast_query.is_simple, ops)
        else:
            # evaluate from deltas
            old_query = fast_query.rebase(db0.snapshot({px_name: last_result[0]}))
            # row groups = (rows removed since last update, rows added since last update) as delta iterators
            query_eval.update((delta(fast_query, old_query), delta(old_query, fast_query)), fast_query.is_simple, ops)
        return query_eval.release()
    
    def format_result(result):
        return {db0.load(key): value.result for key, value in result.items()}
    
    cache = FastQueryCache(prefix=__px_fast_query)
    # take snapshot of the latest known state and rebase input query
    # otherwise refresh might invalidate query results (InvalidStateError)
    with db0.snapshot() as snapshot:
        fast_query = FastQuery(query, group_defs).rebase(snapshot)
        last_result = cache.find_result(fast_query)
        state_num = snapshot.get_state_num(prefix=px_name)
        # return the cached result if from the same state number
        if last_result is None or last_result[0] != state_num:
            result = try_query_eval(fast_query, last_result, ops)
            # update the cache with the result
            last_result = cache.update(state_num, fast_query, result)
        
        # return result from cache (formatted)
        return format_result(last_result[2])
    