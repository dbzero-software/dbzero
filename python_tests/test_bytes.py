# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

import pytest
import dbzero as db0
from datetime import datetime
from dbzero import memo
    
@memo
class BytesMock:
    def __init__(self, bytes):
        self.bytes = bytes

# FIXME: test failing due to incomplete implementation of bytes type (see dropMember / bytes spcecialiation)
# def test_convert_python_bytes_to_bytes_as_member(db0_fixture):
#     bytes_1 = b'some bytes'
#     test_datetime = BytesMock(bytes_1)
#     assert test_datetime.bytes == bytes_1


# FIXME: test failing due to incomplete implementation of bytes type (see dropMember / bytes spcecialiation)
# def test_bytes_member_returned_as_python_datatime(db0_fixture):
#     bytes_1 = bytes(b"test")
#     test_bytes = BytesMock(bytes_1)
#     assert test_bytes.bytes == bytes_1
#     test_bytes.bytes = test_bytes.bytes.replace(b't', b'T')
#     bytes_expected = bytes(b"TesT")
#     assert test_bytes.bytes == bytes_expected
