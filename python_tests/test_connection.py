import dbzero_ce as db0
from .conftest import DB0_DIR


def test_default_prefix_after_connection_open():
    db0.Connection.setup(read_write=True, client_app="app_name", prefix="some-test-prefix")
    db0.Connection.assure_initialized()
    assert db0.get_current_prefix().name == "some-test-prefix"
    db0.Connection.close()