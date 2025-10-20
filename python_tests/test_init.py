import dbzero as db0
from .conftest import DB0_DIR
import shutil
import os


def test_lang_cache_size_can_be_specified_on_init():
    if os.path.exists(DB0_DIR):
        shutil.rmtree(DB0_DIR)    
    os.mkdir(DB0_DIR)
    db0.init(DB0_DIR, config={"lang_cache_size": 9876})
    db0.open("my-test-prefix")    
    assert db0.get_lang_cache_stats()["capacity"] == 9876
    db0.close()
