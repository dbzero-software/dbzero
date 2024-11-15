import dbzero_ce as db0


class Connection:
    db0_dir: str = ''
    db0_default_prefix: str = None
    read_write: bool = True
    client_app = None
    __is_initialized: bool = False
    # configure autocommit on all prefixes
    __config = {
        "autocommit": True,
        "autocommit_interval": 250,
        "cache_size": 4 << 30
    }
    
    @classmethod
    def setup(cls, db0_dir:str = None, prefix:str = None, read_write:bool = True, client_app:str = None,
                autocommit:bool = None, autocommit_interval:int = None, cache_size:int = None, **kwargs):
        cls.db0_dir = db0_dir or cls.db0_dir
        cls.db0_default_prefix = prefix or cls.db0_default_prefix
        cls.read_write = read_write
        cls.client_app = client_app
        if autocommit is not None:
            cls.__config["autocommit"] = autocommit
        if autocommit_interval is not None:
            cls.__config["autocommit_interval"] = autocommit_interval
        if cache_size is not None:
            cls.__config["cache_size"] = cache_size
    
    @classmethod
    def assure_initialized(cls, db0_dir:str = None, prefix:str = None):
        if cls.__is_initialized:
            return
        
        init_dir = db0_dir or cls.db0_dir
        if init_dir is None:
            raise ValueError("db0_dir is not set")
        
        # make init_dir if does not exist
        cls.db0_dir = init_dir
        db0.init(init_dir, cls.__config)
        init_prefix = prefix or cls.db0_default_prefix
        if init_prefix is not None:
            db0.open(init_prefix, "rw" if cls.read_write else "r")
        
        # for client apps the private fq-cache prefix should be opened as default one
        if cls.client_app:
            db0.init_fast_query(prefix=f"__fq_cache/{cls.client_app}")        
        cls.__is_initialized = True
    
    
    @classmethod
    def close(cls):
        if cls.__is_initialized:
            db0.close()
            cls.__is_initialized = False
    