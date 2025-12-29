# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0


def test_can_mark_metod_immutable():
    @db0.immutable
    def foo():
        return 42

    assert db0.is_immutable(foo)
    assert not db0.is_immutable_query(foo)
    assert db0.is_immutable_parameter(foo)
    assert foo() == 42


    @db0.immutable
    def foo(param):
        return param
    assert db0.is_immutable(foo)
    assert db0.is_immutable_query(foo)
    assert not db0.is_immutable_parameter(foo)

    @db0.immutable
    def foo(**kwargs):
        return 42
    assert db0.is_immutable(foo)
    assert db0.is_immutable_query(foo)
    assert not db0.is_immutable_parameter(foo)


    @db0.immutable
    def foo(*args):
        return 42

    assert db0.is_immutable(foo)
    assert db0.is_immutable_query(foo)
    assert not db0.is_immutable_parameter(foo)

    @db0.immutable
    def foo_default(some_arg=None):
        return 42
    assert db0.is_immutable(foo)
    assert db0.is_immutable_query(foo)
    assert not db0.is_immutable_parameter(foo)


def test_immutable_class_params():
    
    class Foo:
        def __init__(self):
            self.param = 42
        @db0.immutable
        def method(self):
            return 42
    obj = Foo()
    assert db0.is_immutable(Foo.method)
    assert db0.is_immutable(obj.method)
    assert obj.param == 42


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


class CompleteWithTest:
    @db0.complete_with('complete_action')
    def before_query(self, param):
        pass

    def complete_action(self, hash, param):
        pass

    @db0.complete_with('complete_missing_action')
    def before_bad_query(self, param):
        pass

@db0.complete_with('complete_with_action')
def complete_with_query(param):
    pass

def complete_with_action(param):
    pass

def test_complete_with():
    assert db0.has_complete_action(CompleteWithTest.before_query)
    assert db0.get_complete_action_name(CompleteWithTest.before_query) == 'complete_action'
    assert db0.get_complete_action(CompleteWithTest.before_query) == CompleteWithTest.complete_action

    assert db0.has_complete_action(CompleteWithTest.before_bad_query)
    with pytest.raises(AttributeError):
        db0.get_complete_action(CompleteWithTest.before_bad_query)

    assert db0.has_complete_action(complete_with_query)
    assert db0.get_complete_action_name(complete_with_query) == 'complete_with_action'
    assert db0.get_complete_action(complete_with_query) == complete_with_action

    class InnerClass:
        @db0.complete_with('action')
        def query(self, param):
            pass

        def action(self, has, param):
            pass

    obj = InnerClass()
    assert db0.has_complete_action(obj.query)
    assert db0.get_complete_action(obj.query) == obj.action
    assert db0.has_complete_action(InnerClass.query)
    with pytest.raises(RuntimeError):
        db0.get_complete_action(InnerClass.query)
