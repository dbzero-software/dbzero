import pytest
import dbzero_ce as db0
from datetime import datetime
from dbzero_ce import memo

def test_datetime(db0_fixture):
    datetime_1 = db0.datetime(2015, 6, 24)
    assert datetime_1 != None
    assert datetime_1.year == 2015
    assert datetime_1.month == 6
    assert datetime_1.day == 24

def test_datetime_compare(db0_fixture):
    datetime_1 = db0.datetime(2015, 6, 24)
    datetime_2 = db0.datetime(2015, 6, 24)
    assert datetime_1 == datetime_2

    datetime_3 = db0.datetime(2015, 6, 25)
    assert datetime_1 != datetime_3
    assert datetime_1 < datetime_3
    assert datetime_3 > datetime_2

    
@memo
class DateTimeMock:
    def __init__(self, date):
        self.date = date

def test_datetime_as_member(db0_fixture):
    datetime_1 = db0.datetime(2015, 6, 24)
    test_datetime = DateTimeMock(datetime_1)
    assert test_datetime.date == datetime_1


def test_convert_python_datetime_to_datetime_as_member(db0_fixture):
    datetime_1 = datetime(2015, 6 , 24 )
    datetime_expected = db0.datetime(2015, 6, 24)
    test_datetime = DateTimeMock(datetime_1)
    assert test_datetime.date == datetime_expected


# def test_datetime_with_all_params(db0_fixture):
#     datetime_2 = db0.datetime(2015, 6, 24, hour=12, minute=30, second=45, microsecond=100)
#     assert datetime_2 != None
#     assert datetime_2.year == 2015
#     assert datetime_2.month == 6
#     assert datetime_2.day == 24
#     assert datetime_2.hour == 12
#     assert datetime_2.minute == 30
#     assert datetime_2.second == 45
#     assert datetime_2.microsecond == 100

# def test_datetime_change_values(db0_fixture):
#     datetime_3 = db0.datetime(2015, 6, 24)
#     assert datetime_3 != None
#     assert datetime_3.year == 2015
#     assert datetime_3.month == 6
#     assert datetime_3.day == 24
#     datetime_3.year = 2016
#     datetime_3.month = 7
#     datetime_3.day = 25
#     assert datetime_3.year == 2016
#     assert datetime_3.month == 7
#     assert datetime_3.day == 25