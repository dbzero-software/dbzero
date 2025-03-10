import pytest
import dbzero_ce as db0


def test_can_mark_metod_immutable():
    @db0.immutable
    def foo():
        return 42

    assert db0.is_immutable(foo)
    assert foo() == 42

    with pytest.raises(RuntimeError) as ex:
        @db0.immutable
        def foo(param):
            return param
    assert "Immutable function must have no positional parameters" in str(ex.value)

    with pytest.raises(RuntimeError) as ex:
        @db0.immutable
        def foo(**kwargs):
            return 42
    assert "Immutable function must have no positional parameters" in str(ex.value)

    with pytest.raises(RuntimeError) as ex:
        @db0.immutable
        def foo(*args):
            return 42
    assert "Immutable function must have no positional parameters" in str(ex.value)

    with pytest.raises(RuntimeError) as ex:
        @db0.immutable
        def foo_default(some_arg=None):
            return 42
    assert "Immutable function must have no positional parameters" in str(ex.value)



def test_can_mark_metod_fulltext_search():
    with pytest.raises(RuntimeError) as ex:
        @db0.fulltext
        def foo():
            return 42
    assert "Fulltext function must have exacly one positional parameter" in str(ex.value)
    @db0.fulltext
    def foo2(param):
        return param

    assert db0.is_fulltext(foo2)

    assert foo2("asd") == "asd"

    with pytest.raises(RuntimeError) as ex:
        @db0.fulltext
        def foo(**kwargs):
            return 42
    assert "Fulltext function must have exacly one positional parameter" in str(ex.value)

    with pytest.raises(RuntimeError) as ex:
        @db0.fulltext
        def foo(*args):
            return 42
    assert "Fulltext function must have exacly one positional parameter" in str(ex.value)
    @db0.fulltext
    def foo_default(some_arg=None):
        return 42
    assert db0.is_fulltext(foo_default)
    

def test_is_fulltext():


    @db0.fulltext
    def foo(param):
        return param
    assert db0.is_fulltext(foo)
    assert not db0.is_fulltext(lambda: 42)
    assert not db0.is_fulltext(42)

    def foo2():
        return 42
    assert not db0.is_fulltext(foo2)

    @db0.immutable
    def foo_inmutale():
        return 42
    assert not db0.is_fulltext(foo_inmutale)