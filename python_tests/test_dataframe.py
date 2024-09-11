import dbzero_ce as db0
import numpy as np
import pandas as pd
from pandas.core.api import StringDtype, Int64Dtype
import pytest


def test_db0_dataframe_can_be_created(db0_fixture):
    df_1 = db0.dataframe()    
    assert df_1 is not None    


# FIXME: test failing due to object lifecycle issue
def test_db0_dataframe_can_add_blocks(db0_fixture):
    df_1 = db0.dataframe()    
    assert df_1 is not None
    df_1.append_block(db0.block())
    # df_1.append_block(db0.block())      
    assert df_1.get_block(0) is not None
    # assert df_1.get_block(1) is not None


def test_db0_can_create_dataframe(db0_fixture):
    df = db0.pandas.DB0DataFrame({"a":["a","b","c","d","e","f"], "b": ["a","b","c","d","e","f"]})    
    assert df.shape == (6, 2)


def test_db0_can_filter_dataframe(db0_fixture):
    df = db0.pandas.DB0DataFrame({"a":[1, 2, 3, 4, 5, 6], "b": ["a","b","c","d","e","f"]})    
    assert df.shape == (6, 2)
    df1 = df[df.a == 4]
    assert df1.shape == (1, 2)    
    assert isinstance(df1._mgr.arrays[0], db0.pandas.BlockInterface)
    df2 = df[df.a < 4]
    assert df2.shape == (3, 2)
    assert isinstance(df2._mgr.arrays[0], db0.pandas.BlockInterface)
    # df2 = df[df.a > 4]
    # assert df2.shape == (2, 2)
    # assert isinstance(df2._mgr.arrays[0], db0.pandas.BlockInterface)
    # df2 = df[df.a <= 4]
    # assert df2.shape == (4, 2)
    # assert isinstance(df2._mgr.arrays[0], db0.pandas.BlockInterface)
    # df2 = df[df.a >= 4]
    # assert df2.shape == (3, 2)
    # assert isinstance(df2._mgr.arrays[0], db0.pandas.BlockInterface)


# def test_db0_dataframe_numerical_operations_on_columns(db0_fixture):
#     data = {
#         'A': [1, 2, 3, 4, 5],
#         'B': [5, 6, 7, 8, 9],
#     }

#     df = db0.pandas.DB0DataFrame(data)
#     df['C'] = df['A'] * df['B']
#     assert list(df['C']) == [5, 12, 21, 32, 45]
#     df['C'] = df['A'] / df['B']
#     expected_values = [0.2, 0.3333333333333333, 0.42857142857142855, 0.5, 0.5555555555555556]
#     assert list(df['C']) == np.all(np.isclose(list(df['C']), expected_values, rtol=1e-6))
#     df['C'] = df['A'] + df['B']
#     assert list(df['C']) == [6, 8, 10, 12, 14]
#     df['C'] = df['A'] - df['B']
#     assert list(df['C']) == [-4, -4, -4, -4, -4]
#     df['C'] = df['A'] ** df['B']
#     assert list(df['C']) == [1, 64, 2187, 65536, 1953125]


# def test_db0_dataframe_memory_usage(db0_fixture):
#     # Memory target in bytes (2 GB)
#     target_memory = 2 * 1024**3  # 2 GB in bytes

#     # Number of columns in the DataFrame
#     num_columns = 10
#     # Data type size in bytes for int64 or float64
#     dtype_size = 8 
#     # Calculate the number of rows needed to reach the target memory
#     num_rows = target_memory // (num_columns * dtype_size)

#     # Create a DataFrame with random integers
#     # df = pd.DataFrame(np.random.randint(0, 100, size=(num_rows, num_columns)), columns=[f'col{i}' for i in range(num_columns)])
#     db0.set_cache_size(target_memory)
#     df = db0.pandas.DB0DataFrame({f'col{i}': np.random.randint(0, 100, size=num_rows) for i in range(num_columns)})

#     memory_usage = df.memory_usage(deep=True).sum()
#     # compare memory usage to set cache size + margin 
#     # TODO change the margin when fixing the related bug
#     assert memory_usage <= db0.get_cache_stats()['capacity'] * 2
#     # print(f"DataFrame memory usage: {df.memory_usage(deep=True).sum() / (1024**3):.2f} GB")