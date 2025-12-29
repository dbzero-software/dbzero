# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

"""dbzero initialization functions"""
from .dbzero import _init, open as dbzero_open

def init(dbzero_root: str, **kwargs) -> None:
    """Initialize the dbzero environment in a specified directory and apply global configurations.

    This function sets up the underlying state management engine.
    It must be called once before interacting with any data.
    If you need to switch to a different working directory, you should first call 
    dbzero.close() and then dbzero.init() again with the new path.

    Parameters
    ----------
    dbzero_root : str
        The path to dbzero data files directory. If the directory doesn't exist, 
        it will be created.
    **kwargs : dict
        Additional keyword arguments:
        * prefix (str) shortcut to open a prefix after initialization
        * read_write (bool, default True) set the open mode for the prefix
        
        Configure global dbzero behavior:
        * autocommit (bool, default True) to enable automatic commits
        * autocommit_interval (int, default 367) for commit interval in milliseconds
        * cache_size (int, default 2 GiB) for main object cache size in bytes
        * lang_cache_size (int, default 1024) for language model data cache size
        * lock_flags (dict) to configure locking behavior when opening the prefix in read-write mode

        Lock flags (dict):
        * blocking (bool, default False) wait when trying to acquire the lock
        * timeout (int) maximum waiting time in seconds when blocking wait is enabled
        * force_unlock (bool, default False) force unlocking of existing lock
    """

    init_kwargs = {}
    
    config_keys = ("autocommit", "autocommit_interval", "cache_size", "lang_cache_size")
    config = {}
    for key in config_keys:
        if key in kwargs:
            config[key] = kwargs[key]

    if config:
        init_kwargs["config"] = config

    if "lock_flags" in kwargs:
        init_kwargs["lock_flags"] = kwargs["lock_flags"]

    _init(dbzero_root, **init_kwargs)

    if "prefix" in kwargs:
        open_mode = "rw" if kwargs.get("read_write", True) else "r"
        dbzero_open(kwargs["prefix"], open_mode=open_mode)
