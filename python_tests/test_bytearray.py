# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import dbzero as db0
import pytest
import sys
from .memo_test_types import MemoTestClass


def test_db0_bytearray_can_be_created(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')    
    assert bytearray_1 is not None


def test_db0_bytearray_can_get_value_by_index(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')    
    assert bytearray_1[0] == 97


def test_db0_bytearray_can_append(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')    
    assert bytearray_1 is not None
    bytearray_1.append(98)
    assert bytearray_1[3] == 98


def test_db0_bytearray_can_extend(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')    
    assert bytearray_1 is not None
    bytearray_1.extend([98, 121])
    assert bytearray_1[3] == 98
    assert bytearray_1[4] == 121


def test_db0_bytearray_can_insert(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')    
    assert bytearray_1 is not None
    assert bytearray_1[1] == 98
    bytearray_1.insert(1, 121)
    assert bytearray_1[1] == 121


def test_capitalize(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    expected = db0.bytearray(b'Abc') 
    capitalized = bytearray_1.capitalize()
    assert capitalized[0] == expected[0]


def test_count(db0_fixture):
    bytearray_1 = db0.bytearray(b'abcabcAbc')
    assert bytearray_1.count(97) == 2
    assert bytearray_1.count(99) == 3

    bytearray_2 = db0.bytearray(b'ababcabcabAbc')
    assert bytearray_2.count(b'abc') == 2
    assert bytearray_2.count(db0.bytearray(b'abc')) == 2

# only Python 3.9+

@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires python3.9 or higher")
def test_removeprefix(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.removeprefix(b'ab') == db0.bytearray(b'c')
    assert bytearray_1.removeprefix(b'bc') == db0.bytearray(b'abc')
    assert bytearray_1.removeprefix(b'abc') == db0.bytearray(b'')


@pytest.mark.skipif(sys.version_info < (3, 9), reason="requires python3.9 or higher")
def test_removesuffix(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.removesuffix(b'ab') == db0.bytearray(b'abc')
    assert bytearray_1.removesuffix(b'bc') == db0.bytearray(b'a')
    assert bytearray_1.removesuffix(b'abc') == db0.bytearray(b'')


def test_decode(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.decode() == 'abc'
    assert bytearray_1.decode('utf-8') == 'abc'


def test_endswith(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.endswith(b'c') == True
    assert bytearray_1.endswith(b'b') == False


def test_find(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.find(b'c') == 2
    assert bytearray_1.find(b'b') == 1
    assert bytearray_1.find(b'd') == -1


def test_index(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.index(b'c') == 2
    assert bytearray_1.index(b'b') == 1


def test_join(db0_fixture):
    bytearray_1 = db0.bytearray(b',')
    assert bytearray_1.join([b'1', b'2', b'3']) == db0.bytearray(b'1,2,3')


def test_replace(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.replace(b'c', b'd') == db0.bytearray(b'abd')
    assert bytearray_1.replace(b'c', b'') == db0.bytearray(b'ab')
    assert bytearray_1.replace(b'c', b'd', 1) == db0.bytearray(b'abd')


def test_rfind(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.rfind(b'c') == 2
    assert bytearray_1.rfind(b'b') == 1
    assert bytearray_1.rfind(b'd') == -1


def test_rindex(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.rindex(b'c') == 2
    assert bytearray_1.rindex(b'b') == 1


def test_rpartition(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.rpartition(b'b') == (bytearray(b'a'), bytearray(b'b'), bytearray(b'c'))
    assert bytearray_1.rpartition(b'd') == (bytearray(b''), bytearray(b''), bytearray(b'abc'))


def test_startswith(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.startswith(b'a') == True
    assert bytearray_1.startswith(b'b') == False


def test_translate(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    table = bytearray.maketrans(b'abc', b'xyz')
    assert bytearray_1.translate(table) == db0.bytearray(b'xyz')


def test_center(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.center(5) == db0.bytearray(b' abc ')
    assert bytearray_1.center(6) == db0.bytearray(b' abc  ')
    assert bytearray_1.center(6, b'*') == db0.bytearray(b'*abc**')


def test_ljust(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.ljust(5) == db0.bytearray(b'abc  ')
    assert bytearray_1.ljust(6) == db0.bytearray(b'abc   ')
    assert bytearray_1.ljust(6, b'*') == db0.bytearray(b'abc***')


def test_lstrip(db0_fixture):
    bytearray_1 = db0.bytearray(b' abc ')
    assert bytearray_1.lstrip() == db0.bytearray(b'abc ')
    assert bytearray_1.lstrip(b' ') == db0.bytearray(b'abc ')


def test_rjust(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.rjust(5) == db0.bytearray(b'  abc')
    assert bytearray_1.rjust(6) == db0.bytearray(b'   abc')
    assert bytearray_1.rjust(6, b'*') == db0.bytearray(b'***abc')


def test_rstrip(db0_fixture):
    bytearray_1 = db0.bytearray(b' abc ')
    assert bytearray_1.rstrip() == db0.bytearray(b' abc')
    assert bytearray_1.rstrip(b' ') == db0.bytearray(b' abc')


def test_split(db0_fixture):
    bytearray_1 = db0.bytearray(b'a b c')
    assert bytearray_1.split() == [bytearray(b'a'), bytearray(b'b'), bytearray(b'c')]
    assert bytearray_1.split(b' ') == [bytearray(b'a'), bytearray(b'b'), bytearray(b'c')]


def test_strip(db0_fixture):
    bytearray_1 = db0.bytearray(b' abc ')
    assert bytearray_1.strip() == db0.bytearray(b'abc')
    assert bytearray_1.strip(b' ') == db0.bytearray(b'abc')


def test_expandtabs(db0_fixture):
    bytearray_1 = db0.bytearray(b'a\tb\tc')
    assert bytearray_1.expandtabs() == db0.bytearray(b'a       b       c')
    assert bytearray_1.expandtabs(2) == db0.bytearray(b'a b c')


def test_isalnum(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc123')
    assert bytearray_1.isalnum() == True
    bytearray_2 = db0.bytearray(b'abc 123')
    assert bytearray_2.isalnum() == False
    bytearray_3 = db0.bytearray(b'')
    assert bytearray_3.isalnum() == False


def test_isalpha(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.isalpha() == True
    bytearray_2 = db0.bytearray(b'abc123')
    assert bytearray_2.isalpha() == False
    bytearray_3 = db0.bytearray(b'')
    assert bytearray_3.isalpha() == False


def test_isascii(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.isascii() == True
    bytearray_2 = db0.bytearray(b'abc123')
    assert bytearray_2.isascii() == True
    bytearray_3 = db0.bytearray(b'abc\x80')
    assert bytearray_3.isascii() == False


def test_isdigit(db0_fixture):
    bytearray_1 = db0.bytearray(b'123')
    assert bytearray_1.isdigit() == True
    bytearray_2 = db0.bytearray(b'abc123')
    assert bytearray_2.isdigit() == False
    bytearray_3 = db0.bytearray(b'')
    assert bytearray_3.isdigit() == False


def test_islower(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.islower() == True
    bytearray_2 = db0.bytearray(b'Abc')
    assert bytearray_2.islower() == False
    bytearray_3 = db0.bytearray(b'')
    assert bytearray_3.islower() == False


def test_isspace(db0_fixture):
    bytearray_1 = db0.bytearray(b' ')
    assert bytearray_1.isspace() == True
    bytearray_2 = db0.bytearray(b' abc')
    assert bytearray_2.isspace() == False
    bytearray_3 = db0.bytearray(b'')
    assert bytearray_3.isspace() == False


def test_istitle(db0_fixture):
    bytearray_1 = db0.bytearray(b'Abc Def')
    assert bytearray_1.istitle() == True
    bytearray_2 = db0.bytearray(b'abc def')
    assert bytearray_2.istitle() == False
    bytearray_3 = db0.bytearray(b'')
    assert bytearray_3.istitle() == False


def test_isupper(db0_fixture):
    bytearray_1 = db0.bytearray(b'ABC')
    assert bytearray_1.isupper() == True
    bytearray_2 = db0.bytearray(b'Abc')
    assert bytearray_2.isupper() == False
    bytearray_3 = db0.bytearray(b'')
    assert bytearray_3.isupper() == False


def test_lower(db0_fixture):
    bytearray_1 = db0.bytearray(b'ABC')
    assert bytearray_1.lower() == db0.bytearray(b'abc')


def test_upper(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc')
    assert bytearray_1.upper() == db0.bytearray(b'ABC')


def test_title(db0_fixture):
    bytearray_1 = db0.bytearray(b'abc def')
    assert bytearray_1.title() == db0.bytearray(b'Abc Def')


def test_swapcase(db0_fixture):
    bytearray_1 = db0.bytearray(b'AbC')
    assert bytearray_1.swapcase() == db0.bytearray(b'aBc')


def test_splitlines(db0_fixture):
    bytearray_1 = db0.bytearray(b'a\nb\nc')
    assert bytearray_1.splitlines() == [bytearray(b'a'), bytearray(b'b'), bytearray(b'c')]
    bytearray_2 = db0.bytearray(b'a\nb\nc\n')
    assert bytearray_2.splitlines() == [bytearray(b'a'), bytearray(b'b'), bytearray(b'c')]


def test_zfill(db0_fixture):
    bytearray_1 = db0.bytearray(b'123')
    assert bytearray_1.zfill(5) == db0.bytearray(b'00123')
    assert bytearray_1.zfill(3) == db0.bytearray(b'123')
    

def test_db0_bytearray_can_be_stored_as_member(db0_fixture):
    ba_1 = db0.bytearray(b'abc')
    _ = MemoTestClass(ba_1)


# FIXME: failing test
# def test_db0_bytearray_create_from_list_issue(db0_fixture):
#     """
#     The test was failing with an exception:  TypeError: bytearray() argument needs to be bytearray
#     """
#     # create python's bytearray from list (to verify it works fine)
#     ba_1 = bytearray(list([1, 2, 3, 4]))
#     ba_2 = db0.bytearray(list[1, 2, 3, 4])
#     assert len(ba_2) == len(ba_1)
#     assert list(ba_1) == [1, 2, 3, 4]


# FIXME: failing test
# def test_db0_bytearray_as_iterable_issue(db0_fixture):
#     """
#     The test was failing with C++ side exception:
#     python_tests/test_bytearray.py::test_db0_bytearray_as_iterable_issue terminate called after throwing an instance of 'db0::InputException'
#     what():  Exception N3db014InputExceptionE thrown in function getItem at /src/dev/src/dbzero/object_model/bytes/ByteArray.cpp, line 42: Index out of range: 4:
#     """
#     ba_1 = db0.bytearray(b'1234')
#     for actual, expected in zip(ba_1, [49, 50, 51, 52]):
#         assert actual == expected
