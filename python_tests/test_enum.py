import dbzero_ce as db0


def test_create_enum_type(db0_fixture):
    Colors = db0.enum("Colors", ["RED", "GREEN", "BLUE"])
    assert Colors is not None
