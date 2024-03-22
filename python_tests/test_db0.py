import dbzero_ce as db0


def test_db0_init_close(db0_fixture):
    pass


def test_db0_can_open_long_prefix_name(db0_fixture):
    prefix_name = "/org/project/dev/some-long-prefix-name"
    db0.open(prefix_name)
    db0.drop(prefix_name)