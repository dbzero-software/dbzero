import dbzero_ce as db0
import os
from .conftest import DB0_DIR


def test_default_prefix_after_connection_open():    
    px_name = "some-test-prefix"
    fname = px_name + ".db0"
    if os.path.exists(fname):
        os.remove(fname)
    
    db0.Connection.setup(read_write=True, client_app="app_name", prefix=px_name)
    db0.Connection.assure_initialized()
    assert db0.get_current_prefix().name == "some-test-prefix"
    db0.Connection.close()