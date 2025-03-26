from .dbzero_ce import begin_locked


class LockedManager:
    def __enter__(self):
        self.__ctx = begin_locked()
        return self.__ctx

    def __exit__(self, exc_type, exc_value, traceback):
        self.__ctx.close()
        self.__ctx = None

    
def locked():
    return LockedManager()
