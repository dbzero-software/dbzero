from .dbzero_ce import begin_atomic


class AtomicManager:
    def __enter__(self):
        self.__ctx = begin_atomic()
        return self.__ctx

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.__ctx.close()
        else:
            self.__ctx.cancel()
        self.__ctx = None        

    
def atomic():
    return AtomicManager()
