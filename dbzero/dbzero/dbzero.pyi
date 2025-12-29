"""
Type stubs for dbzero module.
"""

from typing import Any, Optional, Iterable, Dict, List, Tuple, Union, Callable
from .interfaces import (
    Memo, MemoWeakProxy, QueryObject, Tag, TagSet, EnumValue,
    ListObject, IndexObject, TupleObject, SetObject, DictObject, ByteArrayObject,
    ObjectTagManager, Snapshot
)

# Core workspace management functions

def open(prefix_name: str, open_mode: str = "rw", **kwargs: Any) -> None:
    """Open a data prefix and set it as the current working context.

    This function is the primary way to access a specific dataset within the dbzero environment.
    After a prefix is opened, all subsequent object operations by default occur within it.

    Parameters
    ----------
    prefix_name : str
        The unique name for the data partition you want to open.
    open_mode : {"rw", "r"}, default "rw"
        The mode for opening the prefix. "rw" for read-write mode (allows both 
        reading and modifying objects), "r" for read-only mode (prevents any 
        changes to the data).
    **kwargs : dict
        Additional keyword arguments to configure the prefix's behavior:
        
        * autocommit (bool) to disable automatic commits for this prefix
        * slab_size (int) for memory slab allocation size in bytes
        * meta_io_step_size (int) for metadata I/O operation chunk size
        * lock_flags (dict) to change locking behavior when opening the prefix for read-write

    Examples
    --------
    Basic usage:
    
    >>> dbzero.open("user-profiles")

    Read-only access:
    
    >>> dbzero.open("user-profiles", "r")

    With custom settings:
    
    >>> dbzero.open("transaction-logs", autocommit=False)

    Notes
    -----
    If you try to access an object from a prefix that isn't currently open, 
    dbzero will automatically attempt to open that prefix in read-only mode.
    """
    ...

def close(prefix_name: Optional[str] = None) -> None:
    """Gracefully shut down dbzero, persisting changes and releasing resources.

    When called without arguments, closes all open prefixes and terminates the entire
    dbzero instance. With a prefix argument, closes only that specific prefix.
    After full close, dbzero methods will raise exceptions until re-initialized.

    Parameters
    ----------
    prefix_name : str, optional
        Optional name of the prefix to close.
        If omitted, all prefixes and the dbzero instance are closed.

    Examples
    --------
    Close everything:
    
    >>> dbzero.close()

    Close specific prefix:
    
    >>> dbzero.close("users")

    Notes
    -----
    To use dbzero again after a full close, you must re-initialize with dbzero.init(). 
    Objects that are not referenced anywhere are permanently deleted during garbage collection.
    """
    ...

def commit(prefix_name: Optional[str] = None) -> None:
    """Persist all pending data changes.

    Finalizes the current open transaction, ensuring data is durable and consistent.

    Parameters
    ----------
    prefix_name : str, optional
        Optional specific data prefix to commit.
        If omitted, commits changes for all open prefixes.

    Examples
    --------
    Basic commit:
    
    >>> task = MemoTask(type="ingest")
    >>> dbzero.commit()

    Commit specific prefix:
    
    >>> dbzero.commit("archive_db")

    Notes
    -----
    By default, dbzero has autocommit enabled, so explicit commits are usually unnecessary.
    """
    ...

# Object retrieval and management

def fetch(identifier: Union[str, type], expected_type: Optional[type] = None, prefix: Optional[str] = None) -> Memo:
    """Retrieve a dbzero object instance by its UUID or type (for singletons).

    The fastest way to access an object, operating in constant time O(1).
    It is guaranteed that only one instance of an object exists in memory for a given UUID.

    Parameters
    ----------
    identifier : str or type
        The identifier for the object you want to retrieve.
        
        * UUID string: Returns the specific object instance for that UUID
        * type (singleton class): Returns the unique instance of that singleton
    expected_type : type, optional
        Optional type to validate the retrieved object.
        Raises exception if the fetched object is not an instance of this type.
    prefix : str, optional
        Optional name of the data prefix to fetch the object from.
        Used for retrieving singletons from non-default prefixes.

    Returns
    -------
    Memo
        The requested Memo object instance. Subsequent calls with the same UUID return
        the exact same Python object, not a copy.

    Raises
    ------
    Exception
        If the object cannot be found or type validation fails.

    Examples
    --------
    Fetch by UUID:
    
    >>> obj = MemoTestClass(value=123)
    >>> obj_uuid = dbzero.uuid(obj)
    >>> same_obj = dbzero.fetch(obj_uuid)
    >>> assert obj is same_obj

    Fetch singleton by type:
    
    >>> singleton = dbzero.fetch(MemoTestSingleton)

    With type validation:
    
    >>> obj = dbzero.fetch(obj_uuid, MemoTestClass)

    From specific prefix:
    
    >>> obj = dbzero.fetch(MemoTestSingleton, prefix="my-prefix")
    """
    ...

def exists(identifier: Union[str, type], expected_type: Optional[type] = None, prefix: Optional[str] = None) -> bool:
    """Check if an identifier points to a valid dbzero object or an existing singleton instance

    Can check by UUID or by singleton type.
    Allows to verify if an object is still available before trying to retrieve it.

    Parameters
    ----------
    identifier : str or type
        The identifier for object to check for.
        
        * str: Check for object with its unique identifier
        * type: Check for instance of this singleton type
    expected_type : type, optional
        Optional expected type when checking by UUID.
        Verifies the found object is an instance of this type.
    prefix : str, optional
        Optional prefix name to search within. Defaults to currently active prefix.
        Only used when checking singleton types.

    Returns
    -------
    bool
        True if the object exists (and matches type if specified), False otherwise.

    Examples
    --------
    Check by UUID:
    
    >>> obj = MyDataObject(value="hello")
    >>> uuid = dbzero.uuid(obj)
    >>> assert dbzero.exists(uuid)

    Check with type validation:
    
    >>> assert dbzero.exists(uuid, MyDataObject)
    >>> assert not dbzero.exists(uuid, SomeOtherClass)

    Check singleton:
    
    >>> _ = MySingleton(data="...")
    >>> assert dbzero.exists(MySingleton)

    Check singleton in specific prefix:
    
    >>> assert dbzero.exists(MySingleton, prefix="my-prefix")
    """
    ...

def uuid(obj: Memo, /) -> str:
    """Get the unique object ID (UUID) of a memo instance.

    Returns a stable handle that allows the object to be reliably fetched
    with dbzero.fetch() across sessions. The UUID is a base-32 encoded string.

    Parameters
    ----------
    obj : Memo
        A memo instance or weak proxy to get the UUID of.

    Returns
    -------
    str
        The UUID as a base-32 encoded string.

    Examples
    --------
    Basic usage:
    
    >>> user = User(name="Alex")
    >>> user_uuid = dbzero.uuid(user)
    >>> retrieved_user = dbzero.fetch(user_uuid)
    >>> assert user == retrieved_user

    Getting own UUID during initialization:
    
    >>> @dbzero.memo
    ... class MyObject:
    ...     def __init__(self):
    ...         # Can't use dbzero.uuid(self) directly in __init__
    ...         self.my_uuid = dbzero.uuid(dbzero.materialized(self))
    """
    ...

def load(obj: Any, /, *, exclude: Optional[Union[List[str], Tuple[str, ...]]] = None, **kwargs: Any) -> Any:
    """Load a dbzero instance recursively into memory as its equivalent native Python representation

    Useful for exporting application state for APIs or functions expecting standard Python types, 
    like JSON serialization. Intelligently handles both standard Python and dbzero types.

    Parameters
    ----------
    obj : Any
        The object to convert. Can be native Python type, dbzero object,
        or @dbzero.memo class instance.
    exclude : list of str or tuple of str, optional
        Optional list of attribute names to exclude when loading
        @dbzero.memo class instances. Only works with default serialization.
    **kwargs : dict
        Additional keyword arguments passed to custom __load__ methods.

    Returns
    -------
    Any
        Converted object with the following behavior:
        
        * Native types: Returned as-is
        * dbzero collections: Converted to built-in counterparts (list, tuple, set, dict)
        * @dbzero.enum values: Converted to string representation
        * @dbzero.memo instances: Converted to dictionaries (or using custom __load__ method)

    Raises
    ------
    RecursionError
        If the object contains cyclic references.
    AttributeError
        If exclude is used with custom __load__ methods.

    Examples
    --------
    Convert dbzero collections:
    
    >>> Colors = dbzero.enum("Colors", ["RED", "GREEN", "BLUE"])
    >>> my_list = dbzero.list([1, "string", Colors.GREEN])
    >>> native = dbzero.load(my_list)  # [1, 'string', 'GREEN']

    Convert memo instance:
    
    >>> @dbzero.memo
    ... class User:
    ...     def __init__(self, name, age):
    ...         self.name = name
    ...         self.age = age
    >>> user = User("Alex", 30)
    >>> user_dict = dbzero.load(user)  # {'name': 'Alex', 'age': 30}

    Exclude attributes:
    
    >>> @dbzero.memo
    ... class Book:
    ...     def __init__(self, title, author, isbn):
    ...         self.title = title
    ...         self.author = author
    ...         self.isbn = isbn
    >>> book = Book("The Hobbit", "Tolkien", "978-0345339683")
    >>> book_dict = dbzero.load(book, exclude=["isbn"])  # {'title': ..., 'author': ...}

    Custom loading:
    
    >>> @dbzero.memo
    ... class Report:
    ...     def __init__(self, data, author):
    ...         self.data = data
    ...         self.author = author
    ...     def __load__(self, include_author=True):
    ...         if include_author:
    ...             return {"report_data": self.data, "created_by": self.author}
    ...         return {"report_data": self.data}
    >>> report = Report({"sales": 100}, "Admin")
    >>> full = dbzero.load(report, include_author=True)
    >>> simple = dbzero.load(report, include_author=False)
    """
    ...


def load_all(obj: Any, /, *, exclude: Optional[Union[List[str], Tuple[str, ...]]] = None, **kwargs: Any) -> Any:
    """Load a dbzero instance recursively into memory, ignoring the top-level custom __load__ method.
    
    It works similarly to load() but ignores custom __load__ implementations
    for the top-level object. This is useful for implementing custom __load__ methods where
    we want to retrieve all fields along with additional data (e.g., all fields + UUID).

    Parameters
    ----------
    obj : Any
        The object to convert. Can be native Python type, dbzero object,
        or @dbzero.memo class instance.
    exclude : list of str or tuple of str, optional
        Optional list of attribute names to exclude when loading
        @dbzero.memo class instances. Only works with default serialization.
    **kwargs : dict
        Additional keyword arguments passed to custom __load__ methods.

    Returns
    -------
    Any
        Converted object with the following behavior:
        
        * Native types: Returned as-is
        * dbzero collections: Converted to built-in counterparts (list, tuple, set, dict)
        * @dbzero.enum values: Converted to string representation
        * @dbzero.memo instances: Converted to dictionaries (or using custom __load__ method)

    Raises
    ------
    RecursionError
        If the object contains cyclic references.
    AttributeError
        If exclude is used with custom __load__ methods.

    Examples
    --------
    Loading all fields + UUID:
    
    >>> @db0.memo
    ... @dataclass
    ... class User:
    ...     first_name: str
    ...     last_name: str
    ...     email: str
    ...
    ...     def __load__(self):
    ...         return {
    ...             "uuid": db0..uuid(self),
    ...             **db0.load_all(self)
    ...         }
    """
    ...


def hash(obj: Any, /) -> int:
    """Compute a deterministic 64-bit integer hash for any object.

    Generates a hash value guaranteed to be consistent across different Python
    processes and application restarts. Important for data persistence and reliable
    key lookups in persistent data structures.

    Parameters
    ----------
    obj : Any
        The object to hash. Supported types include:
        
        * Standard Python primitives (str, bytes, int, Decimal)
        * Collections (tuple)
        * datetime objects (date, time, datetime with/without timezones)
        * Python Enum values
        * dbzero counterparts (dbzero.tuple, dbzero.enum, etc.)

    Returns
    -------
    int
        A 64-bit signed integer representing the hash. Objects with the same
        value always produce the same hash.

    Examples
    --------
    Consistent hashing:
    
    >>> assert dbzero.hash("hello world") == dbzero.hash("hello world")
    >>> assert dbzero.hash("hello") != dbzero.hash("world")

    Python types:
    
    >>> from datetime import date
    >>> my_tuple = (1, "string", date(2024, 8, 1))
    >>> hash_value = dbzero.hash(my_tuple)  # Always same result

    dbzero types:
    
    >>> Colors = dbzero.enum("Colors", ["RED", "GREEN", "BLUE"])
    >>> key = dbzero.tuple([100, Colors.RED])
    >>> hash_value = dbzero.hash(key)
    >>> data = dbzero.dict({key: "value"})  # Uses dbzero.hash internally
    """
    ...

def set_prefix(object: Memo, prefix: Optional[str] = None) -> None:
    """Set the persistence prefix for a Memo instance dynamically at runtime.

    Allows to control which data prefix an object belongs to.
    MUST be called as the first statement inside __init__ constructor.

    Parameters
    ----------
    object : Memo
        The class instance being initialized. You should always pass 'self'.
    prefix : str, optional
        Name of the prefix (scope) for the instance. If None, uses current default prefix.

    Examples
    --------
    Reusable scoped singleton:
    
    >>> @dbzero.memo(singleton=True)
    ... class ScopedCache:
    ...     def __init__(self, prefix=None):
    ...         # MUST be first call in constructor!
    ...         dbzero.set_prefix(self, prefix)
    ...         self.data = {}
    ...
    ...     def set(self, key, value):
    ...         self.data[key] = value
    """
    ...

def materialized(obj: Memo, /) -> Memo:
    """Provide a reference to a Memo object that is safe for use within its own __init__.

    Solves the chicken-and-egg problem where an object isn't fully initialized in dbzero
    until its __init__ completes, but you need to reference it during construction.

    Parameters
    ----------
    obj : Memo
        The object instance (typically 'self') being initialized.

    Returns
    -------
    Memo
        A stable handle to the object that can be used with other dbzero functions.

    Examples
    --------
    Index object during initialization:
    
    >>> @dbzero.memo
    ... class Document:
    ...     def __init__(self, content, index):
    ...         self.content = content
    ...         index.add(content, dbzero.materialized(self))

    Generate UUID during initialization:
    
    >>> @dbzero.memo
    ... class User:
    ...     def __init__(self):
    ...         self.id = dbzero.uuid(dbzero.materialized(self))

    Self-referential relationships:
    
    >>> @dbzero.memo
    ... class Node:
    ...     def __init__(self, parent_index):
    ...         self.children = dbzero.list()
    ...         if parent_index:
    ...             parent_index.add("child", dbzero.materialized(self))
    """
    ...

def assign(*objects: Memo, **attributes: Dict[str, Any]) -> None:
    """Perform bulk attribute updates on one or more Memo objects.

    Convenient way to set multiple attributes to new values in a single,
    readable command.

    Parameters
    ----------
    *objects : Memo
        One or more Memo objects whose attributes you want to update.
    **attributes : Dict[str, Any]
        The attributes to update, provided as name=value pairs.

    Examples
    --------
    Update single object:
    
    >>> obj = MemoTestThreeParamsClass(value_1=1, value_2=2, value_3=3)
    >>> dbzero.assign(obj, value_1=4, value_2=5)
    >>> assert obj.value_1 == 4 and obj.value_2 == 5 and obj.value_3 == 3

    Update multiple objects:
    
    >>> obj_1 = MemoTestThreeParamsClass(1, 2, 3)
    >>> obj_2 = MemoTestThreeParamsClass(1, 2, 3)
    >>> dbzero.assign(obj_1, obj_2, value_1=10, value_2=20)
    >>> assert obj_1.value_1 == 10 and obj_2.value_1 == 10
    >>> assert obj_1.value_2 == 20 and obj_2.value_2 == 20

    Batch updates:
    
    >>> users = [user1, user2, user3]
    >>> dbzero.assign(*users, status="active", last_updated=datetime.now())

    Notes
    -----
    This operation is not atomic and changes are applied individually. See: atomic_assign
    """
    ...

def touch(*objects: Memo) -> None:
    """Mark one or more Memo objects as modified without changing their data.

    Parameters
    ----------
    *objects : Memo
        The Memo object(s) to be touched/marked as modified.

    Examples
    --------
    Touch single object:
    
    >>> task = Task("Initial task")
    >>> dbzero.commit()
    >>> snap_1 = dbzero.snapshot()
    >>> dbzero.touch(task)  # Mark as modified without changes
    >>> dbzero.commit()
    >>> snap_2 = dbzero.snapshot()
    >>> modified = dbzero.select_modified(dbzero.find(Task), snap_1, snap_2)
    >>> assert len(modified) == 1
    """
    ...

def rename_field(class_obj: type, from_name: str, to_name: str) -> None:
    """Rename a field for a given Memo class.

    Modifies the internal field layout for all existing and future instances
    of the class. After execution, field is accessible only by its new name.

    Parameters
    ----------
    class_obj : type
        The memo type for which to rename the field.
    from_name : str
        The current name of the field you want to change.
    to_name : str
        The new name for the field.

    Examples
    --------
    Basic field rename:
    
    >>> # Assume DynamicDataClass has fields like 'field_0', 'field_1', etc.
    >>> obj = DynamicDataClass()
    >>> print(obj.field_33)  # Outputs: 33
    >>>
    >>> # Rename field across all instances
    >>> dbzero.rename_field(DynamicDataClass, "field_33", "renamed_field")
    >>>
    >>> # Data now accessible via new name
    >>> print(obj.renamed_field)  # Outputs: 33
    >>> # obj.field_33 would raise AttributeError

    Notes
    -----
    If you call rename_field for a field that has already been renamed, the method 
    will not raise an error and will exit gracefully.
    """
    ...

# Cache management

def clear_cache() -> None:
    """Manually evict all objects from the in-memory cache.

    Examples
    --------
    Free memory after large operation:
    
    >>> print(f"Before: {dbzero.get_cache_stats()['size']} bytes")
    >>>
    >>> # Perform memory-intensive task
    >>> for i in range(10000):
    ...     obj = DataPoint(value=i, metadata="temporary data")
    >>>
    >>> print(f"During task: {dbzero.get_cache_stats()['size']} bytes")
    >>> dbzero.clear_cache()  # Free up memory
    >>> print(f"After clearing: {dbzero.get_cache_stats()['size']} bytes")

    Cleanup between processing phases:
    
    >>> # Process batch 1
    >>> process_large_dataset(batch1)
    >>> dbzero.clear_cache()  # Clean slate for batch 2
    >>> process_large_dataset(batch2)

    Memory-constrained environments:
    
    >>> # Periodic cleanup in long-running processes
    >>> if is_memory_usage_high():
    ...     dbzero.clear_cache()

    Notes
    -----
    This function does NOT delete objects or data from dbzero files, only removes 
    objects from fast, in-memory cache. Objects are reloaded on demand when accessed again.
    """
    ...

def set_cache_size(size: int, /) -> None:
    """Set the maximum size of the in-memory cache in bytes.

    Allows dynamic adjustment of memory ceiling for cache at runtime. Increase
    for better performance by keeping more objects in memory, or decrease to
    reduce process memory footprint.

    Parameters
    ----------
    size : int
        The desired maximum cache size in bytes.

    Notes
    -----
    * If new size < current usage, dbzero automatically evicts objects until limit is met
    * Changes take immediate effect
    * Cache management continues with new size limit
    """
    ...

# Collection creation functions

def list(iterable: Optional[Iterable[Any]] = None, /) -> ListObject:
    """Create a persistent, mutable sequence object.

    Parameters
    ----------
    iterable : Iterable[Any], optional
        Optional iterable to initialize the list with.

    Returns
    -------
    ListObject
        A new ListObject that has the same interface as Python list.

    Examples
    --------
    Create and modify:
    
    >>> tasks = dbzero.list()
    >>> tasks.append("buy milk")
    >>> tasks.append("walk the dog")
    >>> tasks[0] = "buy almond milk"

    Initialize from iterable:
    
    >>> numbers = dbzero.list([10, 20, 30, 40, 50])

    With dbzero objects:
    
    >>> tasks_list = dbzero.list()
    >>> task1 = Task("write docs")
    >>> tasks_list.append(task1)
    """
    ...

def index() -> IndexObject:
    """Create a persistent, ordered data structure for efficient queries.

    An index is like a dictionary where keys are always sorted, enabling fast range scans
    and ordered iteration. It is useful for maintaining ordered collections and sorted queries.

    Returns
    -------
    IndexObject
        A new IndexObject.

    Examples
    --------
    Basic usage:
    
    >>> tasks_index = dbzero.index()
    >>> task1 = Task("Deploy")
    >>> tasks_index.add(1, task1)  # Priority 1 (high)
    >>> tasks_index.add(10, Task("Docs"))  # Priority 10 (low)

    Range queries:
    
    >>> events = dbzero.index()
    >>> events.add(datetime.now(), Event("Now"))
    >>> recent = events.select(yesterday, today)  # Time range
    >>> all_future = events.select(min_key=now)  # Open-ended range

    Sorting query results:
    
    >>> alpha_tasks = dbzero.find("project-alpha")
    >>> sorted_tasks = priority_index.sort(alpha_tasks)

    Multi-level sorting:
    
    >>> by_date_then_priority = priority_index.sort(date_index.sort(all_tasks))

    With None keys:
    
    >>> tasks_index.add(None, Task("TBD"))
    >>> all_with_tbd = tasks_index.select(null_first=True)

    Notes
    -----    
    * Keys determine sort order; values must be dbzero-managed objects
    * Range query keys are inclusive on both sides [min_key, max_key]
    * Calling select() with no args returns all objects
    """
    ...

def tuple(iterable: Iterable[Any] = (), /) -> TupleObject:
    """Create a persistent, immutable sequence object.

    Parameters
    ----------
    iterable : Iterable[Any], default ()
        Optional iterable (list, tuple, generator) to initialize
        the tuple's elements. Defaults to empty tuple if not provided.

    Returns
    -------
    TupleObject
        A new TupleObject that has the same interface as Python tuple.

    Examples
    --------
    Create empty tuple:
    
    >>> empty = dbzero.tuple()
    >>> print(len(empty))  # 0

    From list:
    
    >>> my_tuple = dbzero.tuple([1, "hello", b"world", True])
    >>> print(my_tuple[1])  # "hello"

    From generator:
    
    >>> gen_tuple = dbzero.tuple(i for i in range(3))  # (0, 1, 2)

    Standard operations:
    
    >>> data = dbzero.tuple(['apple', 'banana', 'cherry', 'banana'])
    >>> a, b, c, d = data  # Unpacking
    >>> count = data.count('banana')  # 2
    >>> index = data.index('cherry')  # 2
    >>> assert data == ('apple', 'banana', 'cherry', 'banana')

    With dbzero objects:
    
    >>> obj_tuple = dbzero.tuple([memo_obj1, memo_obj2])
    >>> # Objects held by strong references
    """
    ...

def set(iterable: Optional[Iterable[Any]] = None, /) -> SetObject:
    """Create a persistent, mutable, unordered collection of unique elements.

    Parameters
    ----------
    iterable : Iterable[Any], optional
        Optional iterable (list, tuple, set) to initialize the set.
        If not provided, an empty set is created.

    Returns
    -------
    SetObject
        A new SetObject that has the same interface as Python set.

    Examples
    --------
    Create empty set and add elements:
    
    >>> users = dbzero.set()
    >>> users.add("alice")
    >>> users.add("bob")
    >>> users.add("alice")  # Duplicates ignored
    >>> print(len(users))  # 2

    Initialize from iterable:
    
    >>> numbers = dbzero.set([1, 2, 3, 4, 5, 1, 2])  # {1, 2, 3, 4, 5}

    Set operations:
    
    >>> approved = dbzero.set(["eva", "frank", "grace"])
    >>> applicants = {"grace", "heidi", "ivan"}
    >>> all_users = approved.union(applicants)
    >>> overlap = approved & applicants  # intersection

    With dbzero objects:
    
    >>> memo_objects = dbzero.set()
    >>> memo_objects.add(some_memo_obj)
    """
    ...

def dict(iterable: Optional[Iterable], /, **kwargs: Any) -> DictObject:
    """Create a persistent, mutable mapping object.

    Parameters
    ----------
    iterable : Iterable, optional
        Mapping object or iterable of key-value pairs.
    **kwargs : Any
        Initialize dictionary with keyword arguments

    Returns
    -------
    DictObject
        A new DictObject object that has the same interface as Python dict.

    Examples
    --------
    Create empty dictionary:
    
    >>> d = dbzero.dict()

    From keyword arguments:
    
    >>> d = dbzero.dict(name="Alice", age=30)

    From list of tuples:
    
    >>> d = dbzero.dict([("name", "Bob"), ("age", 25)])

    From another dictionary:
    
    >>> d = dbzero.dict({"name": "Charlie", "age": 35})

    Complex keys and nesting:
    
    >>> stats = dbzero.dict()
    >>> stats[(user_obj, "logins")] = 15
    >>> complex_data = dbzero.dict()
    >>> complex_data["user_a"] = {"permissions": ["read", "write"]}
    """
    ...

def bytearray(source: Union[bytes, Iterable[int]] = b'', /) -> ByteArrayObject:
    """Create a mutable persisted sequence of bytes.

    Parameters
    ----------
    source : bytes or Iterable[int], default b''
        Optional bytes-like object or iterable of integers (0-255)
        to initialize the byte array. Defaults to empty if not provided.

    Returns
    -------
    ByteArrayObject
        A new ByteArrayObject that has the same interface as Python bytearray.

    Examples
    --------
    Initialize from iterable:
    
    >>> data = dbzero.bytearray([0xDE, 0xAD, 0xBE, 0xEF])

    Standard operations:
    
    >>> data = dbzero.bytearray(b'hello world')
    >>> data.upper()  # Modifies in-place
    >>> index = data.find(b'world')
    >>> data.replace(b'world', b'dbzero')
    """
    ...

# Tag and query functions

def tags(*objects: Memo) -> ObjectTagManager:
    """Get a tag manager interface for given Memo objects.

    Parameters
    ----------
    *objects : Memo
        One or more Memo objects to manage tags for.

    Returns
    -------
    ObjectTagManager
        A ObjectTagManager interface for given Memo objects.

    Examples
    --------
    Add single tag:
    
    >>> book = Book("Hitchhiker's Guide")
    >>> dbzero.tags(book).add("sci-fi")

    Add multiple tags:
    
    >>> task = Task("Write docs")
    >>> dbzero.tags(task).add(["docs", "urgent", "writing"])

    Batch operations on multiple objects:
    
    >>> product1, product2 = Product("Laptop"), Product("Mouse")
    >>> dbzero.tags(product1, product2).add("sale")
    >>> dbzero.tags(product1, product2).remove("sale")

    Chain operations:
    
    >>> dbzero.tags(obj).add("tag1").remove("old-tag").add("tag2")
    """
    ...

def find(*query_criteria: Union[Tag, List[Tag], Tuple[Tag], QueryObject, TagSet], prefix: Optional[str] = None) -> QueryObject:
    """Query for memo objects based on search criteria such as tags, types, or subqueries.

    The primary way to search for objects. All top-level criteria are combined
    using AND logic - objects must satisfy all conditions to be included in the query result.

    Parameters
    ----------
    *query_criteria : Union[Tag, List[Tag], Tuple[Tag], QueryObject, TagSet]
        Variable number of criteria to filter objects:
        
        * Type: A class to filter by type (includes subclasses)
        * String tag: Simple string tag
        * Object tag: Any memo object used as a tag
        * List of tags (OR): Objects with at least one of the specified tags
        * Tuple of tags (AND): Objects with all of the specified tags
        * QueryObject: Result of another query
        * TagSet: Set logical operation.
    prefix : str, optional
        Optional data prefix to run the query on.
        If omitted, the prefix to run the query is resolved from query criteria.

    Returns
    -------
    QueryObject
        An iterable query object.

    Examples
    --------
    Find by type:
    
    >>> results = dbzero.find(MemoTestClass)

    Find by tag:
    
    >>> results = dbzero.find("tag1")

    Find with multiple criteria (AND):
    
    >>> results = dbzero.find(MemoTestClass, "tag1")

    Find with OR tags:
    
    >>> results = dbzero.find(['tag1', 'tag2'])  # tag1 OR tag2

    Find with AND tags:
    
    >>> results = dbzero.find(('tag1', 'tag2'))  # tag1 AND tag2

    Find with subquery:
    
    >>> subquery = index.select(100, 200)
    >>> results = dbzero.find(MemoTestClass, "tag1", subquery)

    Find with negation:
    
    >>> results = dbzero.find(query_2, dbzero.no(query_1))

    Find in specific prefix:
    
    >>> results = dbzero.find("tag1", prefix="customer-data")
    """
    ...

def no(predicate: Union[str, QueryObject], /) -> TagSet:
    """Create a negative predicate (NOT condition) for find queries.

    Allows to exclude objects that match the given predicate,
    enabling filtering out unwanted objects based on tags or query results.

    Parameters
    ----------
    predicate : str or QueryObject
        The condition to negate.

    Returns
    -------
    TagSet
        A predicate object representing logical NOT operation.

    Examples
    --------
    Exclude by tag:
    
    >>> # Find active projects but exclude those on hold
    >>> active_not_on_hold = dbzero.find("active-project", dbzero.no("on-hold"))

    Complex exclusions:

    >>> # Find objects with tag1 but not in a specific result set
    >>> excluded_set = dbzero.find("excluded-group")
    >>> filtered = dbzero.find("tag1", dbzero.no(excluded_set))

    Calculate query deltas (find differences):
    
    >>> # Compare snapshots to find changes
    >>> query_1 = snap1.find("some-tag")  # Objects 1, 2
    >>> query_2 = snap2.find("some-tag")  # Objects 2, 3, 4
    >>>
    >>> # Find newly added (in query_2 but NOT in query_1)
    >>> newly_added = snap2.find(query_2, dbzero.no(query_1))  # Objects 3, 4
    """
    ...

def as_tag(obj: Union[Memo, MemoWeakProxy, type]) -> Tag:
    """Make a searchable Tag from a Memo instance or class.

    Allows to use Memo object or class as a label for other objects.
    Tags created from objects are stable identifiers that will
    work even if the original object is deleted.

    Parameters
    ----------
    obj : Union[Memo, type]
        The Memo object or class to convert into a tag.

    Returns
    -------
    Tag
        Objects' tag.

    Examples
    --------
    Using instance as tag:
    
    >>> category_obj = MemoClassForTags("category_A")
    >>> data_obj = MemoClassForTags(101)
    >>> dbzero.tags(data_obj).add(dbzero.as_tag(category_obj))
    >>> results = dbzero.find(dbzero.as_tag(category_obj))
    >>> assert list(results) == [data_obj]

    Using class as tag:
    
    >>> obj = MemoNoDefTags(123)
    >>> dbzero.tags(obj).add(dbzero.as_tag(MemoTestClass))
    >>> # Find objects TAGGED WITH the class (returns 1)
    >>> assert len(dbzero.find(dbzero.as_tag(MemoTestClass))) == 1
    >>> # Find instances OF the class (returns 0)
    >>> assert len(dbzero.find(MemoTestClass)) == 0
    """
    ...

def split_by(tags: List[Tag], query: QueryObject, exclusive: bool = True) -> QueryObject:
    """Transform a query by decorating result items with specified groups,
    such as tags or enum values which they are tagged with.

    Effectively, it categorizes query results. For each item returned by
    the input query that is associated with a group, it additionaly yields
    a tag in a (item, decorator) tuple.

    Parameters
    ----------
    tags : List[Tag]
        A list of tags to split results by.
    query : QueryObject
        The input query whose result set will be categorized.
    exclusive : bool, default True
        Controls handling of items belonging to multiple groups:
        
        * True: Item appears only once, paired with one matching group
        * False: Item appears for every matching group

    Returns
    -------
    QueryObject
        A new query yielding (item, decorator) tuples where item is from
        the original query and decorator is the matched group.

    Examples
    --------
    Split by string tags:
    
    >>> query = dbzero.split_by(["tag1", "tag2", "tag3"], dbzero.find(MemoTestClass))
    >>> for item, tag_decorator in query:
    ...     print(f"Item {item.value} has '{tag_decorator}'")
    >>> # Output: Item 0 has 'tag1', Item 1 has 'tag2', etc.

    Non-exclusive split:
    
    >>> obj = SomeClass()
    >>> dbzero.tags(obj).add(["tag1", "tag2"])  # Object has both tags
    >>>
    >>> # Exclusive (default) - object appears once
    >>> exclusive_query = dbzero.split_by(["tag1", "tag2"], dbzero.find(obj))
    >>> assert len(list(exclusive_query)) == 1
    >>>
    >>> # Non-exclusive - object appears for each matching tag
    >>> non_exclusive = dbzero.split_by(["tag1", "tag2"], dbzero.find(obj), exclusive=False)
    >>> assert len(list(non_exclusive)) == 2

    Compose with other queries:
    
    >>> # Split items by tag3/tag4 from items also tagged with tag1
    >>> split_query = dbzero.split_by(["tag3", "tag4"], dbzero.find("tag1"))
    >>> # Then find items that ALSO have tag2. Grouping decorators from inner query are preserved.
    >>> final_query = dbzero.find(split_query, "tag2")
    >>> for item, decorator in final_query:
    ...     print(f"Item {item.value} matched tag1, tag2, decorated with '{decorator}'")
    """
    ...

def filter(filter: Callable[[Any], bool], query: QueryObject) -> QueryObject:
    """Apply fine-grained, custom filtering logic to a query.

    Useful in situations where complex filtering conditions cannot be expressed
    with tags and dbzero.find(). Works similarly to Python's built-in filter(),
    but seamlessly integrates into the dbzero query pipeline.

    Parameters
    ----------
    filter : Callable[[Any], bool]
        A function or lambda that takes a single object as argument.
        Must return True to include the object, False to exclude it.
    query : QueryObject
        A query to filter.

    Returns
    -------
    QueryObject
        A query that only yields items for which filter function returned True.

    Examples
    --------
    Basic filtering:
    
    >>> # Find objects with "tag1" but only where value == 1
    >>> query = dbzero.filter(lambda x: x.value == 1, dbzero.find("tag1"))
    >>> results = list(query)
    >>> assert len(results) == 1 and results[0].value == 1

    Chain with index sorting:
    
    >>> # Find, filter, then sort using an index
    >>> initial = dbzero.find(MemoTestClass, "tag1")
    >>> filtered = dbzero.filter(lambda x: x.value % 3 != 0, initial)
    >>> query = ix_value.sort(filtered)
    >>> sorted_values = [x.value for x in query]  # [1, 2, 4, 5, 7, 8]

    Advanced text matching:
    
    >>> def find_cities(search_phrase: str):
    ...     # Broad search using tags
    ...     search_tags = list(dbzero.taggify(search_phrase))
    ...     results = cities_index.sort(dbzero.find(search_tags))
    ...     # Precise filtering for in-order token matching
    ...     return dbzero.filter(
    ...         lambda city: match_tokens_in_order(city.name, search_phrase),
    ...         results
    ...     )
    """
    ...

# State and statistics functions

def get_state_num(prefix: Optional[str] = None, finalized: bool = False) -> int:
    """Return the state number for a given data prefix.

    The state number increments with each transaction commit, crucial for tracking
    changes, creating snapshots of specific states, and synchronization tasks.

    Parameters
    ----------
    prefix : str, optional
        Optional name of the prefix to get state number for.
        If None, defaults to the current prefix.
    finalized : bool, default False
        Controls which state number to return:
        
        * False: Returns pending state number (after most recent
          modifications, even if uncommitted)
        * True: Returns last finalized state number (after last successful commit)

    Returns
    -------
    int
        An integer representing state number for the specified prefix.

    Examples
    --------
    Basic usage:
    
    >>> state_1 = dbzero.get_state_num()
    >>> obj = MemoTestClass()
    >>> obj.value = 200
    >>> # After autocommit
    >>> state_2 = dbzero.get_state_num()
    >>> assert state_2 > state_1

    Specific prefix:
    
    >>> state_A = dbzero.get_state_num(prefix="data-prefix-A")
    >>> state_B = dbzero.get_state_num(prefix="data-prefix-B")

    Notes
    -----
    In read-only mode, the function always reports latest finalized state number, regardless of 'finalized' argument.
    """
    ...

# Snapshot functions

def snapshot(state_spec: Optional[Union[int, Dict[str, int]]] = None) -> Snapshot:
    """Get a read-only snapshot view of dbzero state.

    Essential for isolating long-running queries from concurrent writes, analyzing
    past states, or ensuring consistent state for complex operations.

    Parameters
    ----------
    state_spec : int or Dict[str, int], optional
        Specifies which state to capture:
        
        * None (default): Most recently committed state
        * int: Specific historical state number globally
        * dict: Maps prefix names to state numbers for mixed historical views

    Returns
    -------
    Snapshot
        A Snapshot context manager object.

    Examples
    --------
    Basic isolation:
    
    >>> with dbzero.snapshot() as snap:
    ...     results = snap.find("some-tag")  # Isolated from live changes

    Time-travel comparison:
    
    >>> snap_1 = dbzero.snapshot()
    >>> # ... modify data ...
    >>> snap_2 = dbzero.snapshot()
    >>> version_1 = snap_1.fetch(uuid)
    >>> version_2 = snap_2.fetch(uuid)

    Delta queries (find changes):
    
    >>> query1 = snap1.find("tag")
    >>> query2 = snap2.find("tag")
    >>> created = snap2.find(query2, dbzero.no(query1))

    Notes
    -----
    Objects from snapshots are immutable (modifications raise exceptions)
    """
    ...

def get_snapshot_of(obj: Memo, /) -> Snapshot:
    """Get the Snapshot instance from which a given object originates.

    Parameters
    ----------
    obj : Memo
        An object instance previously fetched from a snapshot.

    Returns
    -------
    Snapshot
        The Snapshot object corresponding to the state from which obj was loaded.

    Examples
    --------
    Track object versions across snapshots:
    
    >>> # Get versions from different snapshots
    >>> snap_1 = dbzero.snapshot(state_1)
    >>> snap_2 = dbzero.snapshot(state_2)
    >>> ver_1 = snap_1.fetch(dbzero.uuid(obj))
    >>> ver_2 = snap_2.fetch(dbzero.uuid(obj))
    >>>
    >>> # Get original snapshots for each version
    >>> origin_snap_1 = dbzero.get_snapshot_of(ver_1)
    >>> origin_snap_2 = dbzero.get_snapshot_of(ver_2)
    """
    ...

def is_memo(obj: Any, /) -> bool:
    """Check if a given object is a dbzero memo class or memo instance.

    Parameters
    ----------
    obj : Any
        The object or type to inspect.

    Returns
    -------
    bool
        True if obj is a class decorated with @dbzero.memo or an instance of such a class.
        False for primitive types, standard Python collections, and other dbzero types
        like dbzero.list or dbzero.enum.

    Examples
    --------
    Check memo class and instance:
    
    >>> @dbzero.memo
    ... class MemoClass:
    ...     def __init__(self, value):
    ...         self.value = value
    >>>
    >>> memo_instance = MemoClass(42)
    >>> assert dbzero.is_memo(MemoClass) is True      # Class type
    >>> assert dbzero.is_memo(memo_instance) is True  # Instance

    Check non-memo objects:
    
    >>> class RegularClass:
    ...     pass
    >>> assert dbzero.is_memo(RegularClass) is False
    >>> assert dbzero.is_memo(123) is False
    >>> assert dbzero.is_memo("hello") is False
    >>> assert dbzero.is_memo([1, 2, 3]) is False

    Check other dbzero types:
    
    >>> Colors = dbzero.enum("Colors", ["RED", "GREEN", "BLUE"])
    >>> managed_list = dbzero.list([1, 2, 3])
    >>> assert dbzero.is_memo(Colors.RED) is False
    >>> assert dbzero.is_memo(managed_list) is False
    """
    ...

def is_singleton(obj: Any, /) -> bool:
    """Check if a given object is a dbzero singleton instance.

    Parameters
    ----------
    obj : Any
        The object to inspect.

    Returns
    -------
    bool
        True if the provided object is an instance of a singleton class, False otherwise.

    Examples
    --------
    Define singleton and regular classes:
    
    >>> @dbzero.memo
    ... class User:
    ...     def __init__(self, name: str):
    ...         self.name = name
    >>>
    >>> @dbzero.memo(singleton=True)
    ... class AppConfig:
    ...     def __init__(self, theme: str):
    ...         self.theme = theme

    Check singleton status:
    
    >>> assert dbzero.is_singleton(user_alice) is False
    >>> assert dbzero.is_singleton(app_settings) is True
    """
    ...

def is_enum(value: Any, /) -> bool:
    """Check if an object is a dbzero enum type.

    Parameters
    ----------
    value : Any
        The object to inspect.

    Returns
    -------
    bool
        True if the provided value is a dbzero enum type.
        Returns False for instances (members) of an enum.

    Examples
    --------
    Check enum type vs member:
    
    >>> Colors = dbzero.enum("Colors", ["RED", "GREEN", "BLUE"])
    >>>
    >>> # Check enum type itself
    >>> assert dbzero.is_enum(Colors) is True
    >>>
    >>> # Check enum member (value)
    >>> assert dbzero.is_enum(Colors.RED) is False
    """
    ...

def is_enum_value(value: Any, /) -> bool:
    """Check if an object is a dbzero enum value.

    Parameters
    ----------
    value : Any
        The object to inspect.

    Returns
    -------
    bool
        True if the provided value is a member of an enum created with dbzero.enum().
        Returns False for other data types, class instances, or the enum type itself.

    Examples
    --------
    Check enum member vs type:
    
    >>> Colors = dbzero.enum("Colors", ["RED", "GREEN", "BLUE"])
    >>>
    >>> # Check enum member (value)
    >>> assert dbzero.is_enum_value(Colors.RED) is True
    >>>
    >>> # Check enum type itself
    >>> assert dbzero.is_enum_value(Colors) is False
    >>>
    >>> # Check other data types
    >>> assert dbzero.is_enum_value("RED") is False
    """
    ...

def get_schema(cls: type, /) -> Dict[str, Dict[str, Any]]:
    """Introspect all in-memory instances of a @dbzero.memo class to deduce dynamic schema.

    Provides current overview of attributes and their most common data types
    across all objects of the class. Schema adapts to runtime changes.

    Parameters
    ----------
    cls : type
        A class decorated with @dbzero.memo for which to generate the schema.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Dictionary where keys are attribute names found across all instances.
        Values are metadata dictionaries containing type information.

    Examples
    --------
    Basic schema inference:
    
    >>> @dbzero.memo
    ... class Car:
    ...     def __init__(self, brand, model, year, photo):
    ...         self.brand = brand
    ...         self.model = model
    ...         self.year = year
    ...         self.photo = photo
    >>>
    >>> # Create instances with varied data types
    >>> Car("Toyota", "Corolla", 2020, "https://example.com/toyota.jpg")  # str
    >>> Car("BMW", "X5", 2021, "https://example.com/bmw.jpg")             # str
    >>> Car("Audi", "A4", 2022, b'\\x89PNG...')                           # bytes
    >>> Car("Mercedes", "C-Class", 2023, None)                            # None
    >>>
    >>> schema = dbzero.get_schema(Car)
    >>> # schema['photo']['primary_type'] will be <class 'str'> (most common)

    Dynamic attributes tracking:
    
    >>> car = Car("Honda", "Civic", 2024, None)
    >>> car.service_due_date = "2025-10-01"  # Add attribute at runtime
    >>>
    >>> updated_schema = dbzero.get_schema(Car)
    >>> # Now includes 'service_due_date' with primary_type <class 'str'>

    Notes
    -----
    The output of get_schema() is not static. It is calculated on-the-fly and reflects 
    the exact state of your objects at the moment of the call. The schema will change 
    if you modify attribute types, add new attributes to instances, or delete objects.
    """
    ...

def get_config() -> Dict[str, Any]:
    """Retrieve the active configuration settings for dbzero.

    Get the configuration currently in use, including both parameters
    provided during dbzero.init() and default values for unspecified parameters.

    Returns
    -------
    Dict[str, Any]
        Dictionary containing current configuration key-value pairs:
        
        * autocommit (bool): True if autocommit enabled. Defaults to True.
        * autocommit_interval (int): Milliseconds between commits. Defaults to 250.

    Raises
    ------
    Exception
        If called after the dbzero has been closed with dbzero.close().

    Examples
    --------
    Basic configuration inspection:
    
    >>> import dbzero
    >>> dbzero.init("/tmp/mydb")
    >>> config = dbzero.get_config()
    >>> print(config)
    {'autocommit': True, 'autocommit_interval': 250}

    Custom configuration retrieval:
    
    >>> dbzero.init("/tmp/mydb", config={
    ...     'autocommit': False,
    ...     'autocommit_interval': 1000
    ... })
    >>> config = dbzero.get_config()
    >>> print(config)
    {'autocommit': False, 'autocommit_interval': 1000}

    Notes
    -----
    Configuration is read-only; use dbzero.init() to change settings.
    """
    ...

# Serialization functions

def serialize(obj: Union[QueryObject, EnumValue], /) -> bytes:
    """Convert a dbzero query iterable or enum value into platform-independent binary representation.

    Parameters
    ----------
    obj : Any
        The dbzero object to serialize:
        
        * Query iterable
        * dbzero enum value

    Returns
    -------
    bytes
        A bytes object containing the serialized representation that can be stored,
        transmitted, or used to reconstruct the object later.

    Examples
    --------
    Serialize a query:
    
    >>> for i in range(10):
    ...     obj = MemoTestClass(i)
    ...     dbzero.tags(obj).add("group_a")
    >>> query = dbzero.find("group_a")
    >>> serialized_bytes = dbzero.serialize(query)
    >>> reconstituted = dbzero.deserialize(serialized_bytes)
    >>> assert len(list(reconstituted)) == 10

    Serialize enum values:
    
    >>> Colors = dbzero.enum("Colors", ["RED", "GREEN", "BLUE"])
    >>> serialized_red = dbzero.serialize(Colors.RED)
    >>> deserialized = dbzero.deserialize(serialized_red)
    >>> assert deserialized == Colors.RED
    """
    ...

def deserialize(data: bytes, /) -> Any:
    """Reconstruct a dbzero object from serialized bytes.

    Parameters
    ----------
    data : bytes
        The bytes object previously created by dbzero.serialize().

    Returns
    -------
    Any
        A dbzero object that was encoded in the data bytes.
    """
    ...

# Synchronization functions

def wait(prefix: str, state: int, timeout: Optional[int] = None) -> bool:
    """Block execution until desired prefix reaches target state or timeout occurs.

    Low-level mechanism for synchronizing processes by waiting on data updates.
    The input state number typically comes from another writer process, with which
    we want to synchronize.

    Parameters
    ----------
    prefix : str
        Name of the prefix to monitor for changes.
        Use dbzero.get_current_prefix().name for current prefix.
    state : int
        Target state number to wait for.
        Use dbzero.get_state_num(prefix) for current state.
    timeout : int, optional
        Maximum time to wait in milliseconds.

    Returns
    -------
    bool
        True if prefix reached/surpassed target state_num within timeout.
        False if timeout occured. Returns True immediately if current state
        is already >= target state_num.

    Examples
    --------
    Basic synchronization:
    
    >>> # Writer process commits transactions
    >>> def writer_process(prefix_name):
    ...     dbzero.init(DB0_DIR)
    ...     dbzero.open(prefix_name, "rw")
    ...     for i in range(10):
    ...         obj = SomeClass(i)
    ...         dbzero.commit()

    >>> # Reader process waits for specific state
    >>> dbzero.init(DB0_DIR)
    >>> dbzero.open("my_prefix", "r")
    >>> prefix_name = dbzero.get_current_prefix().name
    >>> initial_state = dbzero.get_state_num(prefix_name)
    >>> target_state = initial_state + 5
    >>>
    >>> # Wait up to 2 seconds for 5 more transactions
    >>> success = dbzero.wait(prefix_name, target_state, 2000)
    >>> if success:
    ...     print(f"Reached state {dbzero.get_state_num(prefix_name)}")
    ... else:
    ...     print("Timeout occurred")

    Notes
    -----
    For async applications, use async_wait() which returns awaitable.
    """
    ...

# Object lifecycle functions

def getrefcount(obj: Union[Memo, type]) -> int:
    """Get the number of strong references to a memo object or class.

    Low-level utility useful for debugging and understanding memory management within dbzero.
    It tells how many other objects, tags, or class instantiations are "pointing" to
    a given object or class, keeping it alive in memory.

    Parameters
    ----------
    obj : Any
        A memo object instance or class to inspect.

    Returns
    -------
    int
        Total number of strong references. A count of 0 for an object means
        it's eligible for garbage collection at the next opportunity.

    Examples
    --------
    Basic object reference counting:
    
    >>> object_B = MemoTestClass(value=200)
    >>> print(dbzero.getrefcount(object_B))  # Output: 0
    >>>
    >>> # object_A now holds a strong reference to object_B
    >>> object_A = MemoTestClass(value=object_B)
    >>> print(dbzero.getrefcount(object_B))  # Output: 1
    >>>
    >>> # Removing the reference decreases the count
    >>> object_A.value = None
    >>> dbzero.commit()
    >>> print(dbzero.getrefcount(object_B))  # Output: 0

    Class instance counting:
    
    >>> initial_count = dbzero.getrefcount(MemoTestClass)
    >>> obj1 = MemoTestClass(value=1)
    >>> obj2 = MemoTestClass(value=2)
    >>> print(dbzero.getrefcount(MemoTestClass))  # initial_count + 2

    Effect of tags on reference count:
    
    >>> my_object = MemoTestClass(value=300)
    >>> print(dbzero.getrefcount(my_object))  # Output: 0
    >>>
    >>> dbzero.tags(my_object).add("important")
    >>> dbzero.commit()
    >>> print(dbzero.getrefcount(my_object))  # Output: 1
    >>>
    >>> dbzero.tags(my_object).add("archive")
    >>> dbzero.commit()
    >>> print(dbzero.getrefcount(my_object))  # Output: 2

    Weak references are ignored:
    
    >>> obj_1 = MemoTestPxClass(123)
    >>> print(f"Refcount: {dbzero.getrefcount(obj_1)}")  # Output: 0
    >>>
    >>> # Create obj_2 with WEAK reference to obj_1
    >>> obj_2 = MemoTestPxClass(dbzero.weak_proxy(obj_1))
    >>> print(f"Refcount after weak ref: {dbzero.getrefcount(obj_1)}")  # Still 0

    Notes
    -----
    Reference count is incremented by:
    
    * Object-to-Object References: When one memo object holds reference to another
    * Tagging: Each tag added with dbzero.tags(obj).add(...) acts as a reference
    * Class Instantiation: For classes, count increases for every instance created
    """
    ...

def weak_proxy(obj: Memo) -> MemoWeakProxy:
    """Create a weak reference to a Memo object.

    Allows storing reference to an object without increasing its reference count.
    Crucial for preventing circular dependencies and enabling cross-prefix references.

    Parameters
    ----------
    obj : Memo
        A dbzero managed object to create a weak reference to.

    Returns
    -------
    MemoWeakProxy
        A special weak proxy object that behaves like the original for most operations
        but doesn't extend the original objects' lifetime.

    Examples
    --------
    Cross-prefix reference:
    
    >>> obj_1 = MemoTestPxClass(123, prefix="default")
    >>> assert dbzero.getrefcount(obj_1) == 0
    >>>
    >>> dbzero.open("another-prefix", "rw")
    >>> obj_2 = MemoTestPxClass(dbzero.weak_proxy(obj_1), prefix="another-prefix")
    >>> assert dbzero.getrefcount(obj_1) == 0  # Still 0 - no strong reference
    >>> assert obj_2.value.value == 123  # Access through proxy

    Handle expired reference:
    
    >>> obj_1 = MemoTestPxClass(123)
    >>> obj_2 = MemoTestPxClass(dbzero.weak_proxy(obj_1))
    >>> del obj_1
    >>> dbzero.commit()  # obj_1 garbage collected
    >>>
    >>> assert dbzero.expired(obj_2.value) is True
    >>> # This raises dbzero.ReferenceError:
    >>> # _ = obj_2.value.value
    >>>
    >>> # But lookups still work with expired proxy:
    >>> obj_3 = MemoTestPxClass(999)
    >>> dbzero.tags(obj_3).add(dbzero.as_tag(obj_2.value))  # Using expired proxy
    >>> results = dbzero.find(dbzero.as_tag(obj_2.value))
    >>> assert list(results) == [obj_3]
    """
    ...

def expired(proxy_object: MemoWeakProxy) -> bool:
    """Check if a weak reference proxy has expired (the object was garbage collected)

    Used to determine if the original object still exists and can be accessed.

    Parameters
    ----------
    proxy_object : MemoWeakProxy
        The weak proxy object to check.

    Returns
    -------
    bool
        True if the object referenced by the proxy no longer exists.
        False if the object is still present in the store.

    Examples
    --------
    Basic expiration check:
    
    >>> original_obj = MyClass(value=123)
    >>> wrapper_obj = OtherClass(ref=dbzero.weak_proxy(original_obj))
    >>>
    >>> assert not dbzero.expired(wrapper_obj.ref)  # Not expired yet
    >>>
    >>> del original_obj
    >>> dbzero.commit()  # Garbage-collect original_obj
    >>>
    >>> assert dbzero.expired(wrapper_obj.ref) is True  # Now expired
    """
    ...
