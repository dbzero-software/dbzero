import dbzero_ce as db0
import argparse
import importlib
import inspect


def list_queries(module_name):
    pass

    
def list_functions_from_module(module_name):
    # Dynamically import the module
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        print(f"Module '{module_name}' not found.")
        return
    
    # Get all the functions from the module
    functions = inspect.getmembers(module, inspect.isfunction)
    
    # Iterate through each function and print its name with parameters
    for function_name, function_obj in functions:
        signature = inspect.signature(function_obj)
        print(f"Function: {function_name}")
        print(f"Parameters: {[param.name for param in signature.parameters.values() if param.kind != inspect.Parameter.VAR_KEYWORD]}")
        has_kwargs = any(
            param.kind == inspect.Parameter.VAR_KEYWORD
            for param in signature.parameters.values())
        print(f"**kwargs: {has_kwargs}")
        print()
    
    
def __main__():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default=None, type=str, help="Location of dbzero files")
    parser.add_argument('--queries', type=str, help="Module containing queries")
    args = parser.parse_args()
    try:
        db0.init(path=args.path)
        list_functions_from_module(args.queries)        
    except Exception as e:
        print(e)
    db0.close()

    
if __name__ == "__main__":
    __main__()
    
