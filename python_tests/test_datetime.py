import pytest
import dbzero_ce as db0
from datetime import datetime, timezone, timedelta
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

def test_datetime_member_with_timezone(db0_fixture):
    datetime_1 = datetime(2015, 6 , 24 , tzinfo=timezone.utc)
    datetime_expected = datetime(2015, 6, 24, tzinfo=timezone.utc)
    test_datetime = DateTimeMock(datetime_1)
    assert test_datetime.date == datetime_expected
    test_datetime.date = test_datetime.date.replace(year=2016)
    datetime_expected = datetime(2016, 6, 24, tzinfo=timezone.utc)
    assert test_datetime.date == datetime_expected
    for i in range (0,24):
        print("TIMEZONE:", i)
        tzinfo = timezone(timedelta(hours=i))
        datetime_1 = datetime(2015, 6 , 24 , tzinfo=tzinfo)
        datetime_expected = datetime(2015, 6, 24, tzinfo=tzinfo)
        test_datetime = DateTimeMock(datetime_1)
        print(f"test_datetime.date: {test_datetime.date}")
        print(f"datetime_expected: {datetime_expected}")
        assert test_datetime.date == datetime_expected

    for i in range (0,24):
        print("TIMEZONE:", i)
        tzinfo = timezone(-timedelta(hours=i))
        datetime_1 = datetime(2015, 6 , 24 , tzinfo=tzinfo)
        datetime_expected = datetime(2015, 6, 24, tzinfo=tzinfo)
        print(f"test_datetime.date: {test_datetime.date}")
        print(f"datetime_expected: {datetime_expected}")
        test_datetime = DateTimeMock(datetime_1)
        print(f"test_datetime.date: {test_datetime.date}")
        assert test_datetime.date == datetime_expected

    tzinfo = timezone(-timedelta(hours=3))
    datetime_1 = datetime(2015, 6 , 24 , tzinfo=tzinfo)
    tzinfo = timezone(-timedelta(hours=5))
    datetime_not_expected = datetime(2015, 6, 24, tzinfo=tzinfo)
    assert test_datetime.date != datetime_not_expected