import dbzero_ce as db0

def test_taggify():
    assert list(db0.taggify("--Mińsk Mazowiecki", max_len = 4)) == ['MINS', 'MAZO']
    assert list(db0.taggify("Markowski, Marek", max_len = 3)) == ['MAR']
    assert list(db0.taggify("A.Kowalski", min_len = 3)) == ['KOW']
    assert list(db0.taggify("A.Kowalski", max_len = None)) == ['A', 'KOWALSKI']
