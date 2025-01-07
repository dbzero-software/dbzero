import os

def __bootstrap__():
    global __bootstrap__, __loader__, __file__
    import imp
    paths = [os.path.join(os.path.split(__file__)[0]), "/src/dev/build/release", "/usr/local/lib/python3/dist-packages/dbzero_ce/"]
    __file__ = None
    for path in paths:
        if os.path.isdir(path):
            for file in os.listdir(path):
                if "dbzero_ce" in file and ("pyd" in file or '.so' in file):
                    full_file_name = os.path.join(path, file)
                    if os.path.isfile(full_file_name):
                        __file__ = full_file_name
                        __loader__ = None
                        del __bootstrap__, __loader__
                        imp.load_dynamic(__name__, __file__)
                        return
        
    if __file__ is None:
        raise Exception("dbzero_ce library not found")
    


__bootstrap__()
