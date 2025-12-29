# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import asyncio
from typing import List, Tuple
from .dbzero import begin_locked, _async_wait, get_config, commit


def async_wait(prefix: str, state_num: int) -> asyncio.Future:
    """Pause an asyncio coroutine until a specific data prefix reaches a target state number.

    Async variant of `dbzero.wait` function, suitable for use in coroutines.

    Parameters
    ----------
    prefix : str
        Name of the prefix to monitor for changes.
    state_num : int
        Target state number to wait for.

    Returns
    -------
    asyncio.Future[None]
        An awaitable object (asyncio.Future).
        Awaiting this future blocks the coroutine until prefix state number is reached.

    Examples
    --------
    Waiting for the next state change:
    
    >>> import asyncio
    >>> # Get the current state number of the default prefix
    >>> current_state = dbzero.get_state_num("default")
    >>> print("Waiting for the next commit...")
    >>> 
    >>> # In another part of your code, make and commit a change
    >>> obj = MyMemoClass(value="initial")
    >>> obj.value = "updated"  # This mutation will increment the state number on commit
    >>> # dbzero automatically commits the change
    >>> 
    >>> await dbzero.async_wait("default", current_state + 1)
    >>> print("State change detected!")

    Timeout handling:
    
    >>> import asyncio
    >>> prefix_name = "default"
    >>> current_state = dbzero.get_state_num(prefix_name)
    >>> 
    >>> # Wait for a future state with timeout protection
    >>> try:
    ...     await asyncio.wait_for(
    ...         asyncio.shield(dbzero.async_wait(prefix_name, current_state + 1)),
    ...         timeout=5.0
    ...     )
    ...     print("State change detected within timeout!")
    ... except asyncio.TimeoutError:
    ...     print("Timeout: No state change occurred within 5 seconds")
    """
    future = asyncio.get_running_loop().create_future()
    _async_wait(future, prefix, state_num)
    return future


async def await_commit(mutation_log: List[Tuple[str, int]]):
    if mutation_log:
        if get_config()['autocommit']:
            for prefix_name, state_number in mutation_log:
                await async_wait(prefix_name, state_number)
        else:
            # To ensure expected behavior, we make explicit commit when autocommit is disabled
            for prefix_name, _state_number in mutation_log:
                commit(prefix_name)


class LockedManager:
    """Locked context manager class"""

    def __init__(self, await_commit):
        self.__await_commit = await_commit

    def __enter__(self):
        if self.__await_commit:
            raise RuntimeError('await_commit is supported only in async context')
        self.__ctx = begin_locked()
        return self.__ctx

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.__ctx.close()
        self.__ctx = None
    
    async def __aenter__(self):
        self.__ctx = begin_locked()
        return self.__ctx

    async def __aexit__(self, exc_type, _exc_value, _traceback):
        self.__ctx.close()
        if self.__await_commit and exc_type is None:
            await await_commit(self.__ctx.get_mutation_log())
        self.__ctx = None


def locked(await_commit: bool = False) -> LockedManager:
    """Blocks the autocommit, ensuring that all changes will be made in a scope of single transaction.

    Allows to capture information about prefix modifications and their current state. This information
    can be used for synchronizing reader processes or analyzing what mutatons take place in a block of
    operations.

    Parameters
    ----------
    await_commit : bool, default False
        If True, the context manager will wait asynchronously until changes made in the locked
        block are committed. Setting this parameter to True is only allowed in `async with` statement.

        **With autocommit disabled, the commit is triggered automatically when closing context manager
        to ensure expected behavior**

    Returns
    -------
    LockedManager
        A context manager that can be used with either `with` (synchronous) or 
        `async with` (asynchronous) statements.

    Examples
    --------
    Synchronous mutation analysis:
    
    >>> # Create some objects in different prefixes
    >>> obj_1 = MemoTestClass(951)
    >>> dbzero.open("some-new-prefix", "rw")
    >>> obj_2 = MemoTestClass(952)
    >>> 
    >>> # Use locked() to track changes
    >>> with dbzero.locked() as lock:
    ...     # A read operation does not create a log entry
    ...     x = obj_1.value
    ...     # A mutating operation does
    ...     obj_2.value = 123123
    >>> 
    >>> # After the block, get the mutation log
    >>> mutation_log = lock.get_mutation_log()
    >>> # The log shows that only "some-new-prefix" was modified
    >>> assert len(mutation_log) == 1
    >>> assert mutation_log[0][0] == "some-new-prefix"

    Asynchronous commit waiting:
    
    >>> obj = MemoTestClass(1234)
    >>> dbzero.commit()  # Ensure initial state is saved
    >>> 
    >>> # Get the number that the *next* commit will have
    >>> next_state_num = dbzero.get_state_num()
    >>> 
    >>> # This async block will pause until the change to `obj` is committed
    >>> async with dbzero.locked(await_commit=True):
    ...     obj.value = 5678
    >>> 
    >>> # Code here will only run after the commit is complete
    >>> print("Commit finished!")
    >>> # We can verify that the state number has advanced as expected
    >>> assert dbzero.get_state_num(finalized=True) == next_state_num

    Notes
    -----
    This function can be used as a lightweight alternative to `dbzero.atomic`,
    with a difference that it doesn't revert changes in case an error occurs. 
    """
    return LockedManager(await_commit)
