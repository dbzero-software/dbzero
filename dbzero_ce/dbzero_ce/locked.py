import asyncio
from typing import List, Tuple
from .dbzero_ce import begin_locked, _await_prefix_state


def await_prefix_state(prefix_name, state_number):
    future = asyncio.get_running_loop().create_future()
    _await_prefix_state(future, prefix_name, state_number)
    return future


async def await_commit(mutation_log: List[Tuple[str, int]]):
    if mutation_log:
        for prefix_name, state_number in mutation_log:
            await await_prefix_state(prefix_name, state_number)


class LockedManager:
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


def locked(await_commit=False):
    return LockedManager(await_commit)
