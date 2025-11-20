import os
import importlib.util as utils


def load_dynamic(name, path):
    spec = utils.spec_from_file_location(name, path)
    module = utils.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def __bootstrap__():
    global __bootstrap__, __loader__, __file__
    paths = [os.path.join(os.path.split(__file__)[0]), "/src/dev/build/release", "/usr/local/lib/python3/dist-packages/dbzero/"]
    __file__ = None
    for path in paths:
        if os.path.isdir(path):
            for file in os.listdir(path):
                if "dbzero" in file and ("pyd" in file or '.so' in file):
                    full_file_name = os.path.join(path, file)
                    if os.path.isfile(full_file_name):
                        __file__ = full_file_name
                        __loader__ = None
                        del __bootstrap__, __loader__
                        load_dynamic(__name__, __file__)
                        return
        
    if __file__ is None:
        raise Exception("dbzero library not found")
    


__bootstrap__()
