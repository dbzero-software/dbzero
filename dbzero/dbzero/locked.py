import asyncio
from typing import List, Tuple
from .dbzero import begin_locked, _async_wait, get_config, commit


def async_wait(prefix_name, state_number):
    future = asyncio.get_running_loop().create_future()
    _async_wait(future, prefix_name, state_number)
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
