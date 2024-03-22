import dbzero_ce as db0
from pandas.core.api import StringDtype, Int64Dtype


def test_db0_dataframe_can_be_created(db0_fixture):
    df_1 = db0.dataframe()    
    assert df_1 is not None    


def test_db0_dataframe_can_add_blocks(db0_fixture):
    df_1 = db0.dataframe()    
    assert df_1 is not None
    df_1.append_block(db0.block())
    df_1.append_block(db0.block())      
    assert df_1.get_block(0) is not None
    assert df_1.get_block(1) is not None


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

