import pytest
import dbzero_ce as db0
from datetime import datetime
from dbzero_ce import memo
    
@memo
class DateTimeMock:
    def __init__(self, date):
        self.date = date

def test_convert_python_datetime_to_datetime_as_member(db0_fixture):
    datetime_1 = datetime(2015, 6 , 24 )
    datetime_expected = datetime(2015, 6, 24)
    test_datetime = DateTimeMock(datetime_1)
    assert test_datetime.date == datetime_expected


def test_datetime_member_returned_as_python_datatime(db0_fixture):
    datetime_1 = datetime(2015, 6 , 24 )
    datetime_expected = datetime(2015, 6, 24)
    test_datetime = DateTimeMock(datetime_1)
    assert test_datetime.date == datetime_expected
    test_datetime.date = test_datetime.date.replace(year=2016)
    datetime_expected = datetime(2016, 6, 24)
    assert test_datetime.date == datetime_expected