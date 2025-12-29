# SPDX-License-Identifier: LGPL-2.1-or-later
# Copyright (c) 2025 DBZero Software sp. z o.o.

from datetime import datetime, timezone, timedelta
from dbzero import memo
    
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

def test_datetime_member_returned_as_python_datatime_with_microseconds(db0_fixture):
    datetime_1 = datetime(2015, 6 , 24, 12, 30, 45, 123456)
    datetime_expected = datetime(2015, 6, 24, 12, 30, 45, 123456)
    test_datetime = DateTimeMock(datetime_1)
    assert test_datetime.date == datetime_expected
    test_datetime.date = test_datetime.date.replace(year=2016)
    datetime_expected = datetime(2016, 6, 24, 12, 30, 45, 123456)
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
        tzinfo = timezone(timedelta(hours=i))
        datetime_1 = datetime(2015, 6 , 24 , tzinfo=tzinfo)
        datetime_expected = datetime(2015, 6, 24, tzinfo=tzinfo)
        test_datetime = DateTimeMock(datetime_1)
        assert test_datetime.date == datetime_expected

    for i in range (0,24):
        tzinfo = timezone(-timedelta(hours=i))
        datetime_1 = datetime(2015, 6 , 24 , tzinfo=tzinfo)
        datetime_expected = datetime(2015, 6, 24, tzinfo=tzinfo)
        test_datetime = DateTimeMock(datetime_1)
        assert test_datetime.date == datetime_expected

    tzinfo = timezone(-timedelta(hours=3))
    datetime_1 = datetime(2015, 6 , 24 , tzinfo=tzinfo)
    tzinfo = timezone(-timedelta(hours=5))
    datetime_not_expected = datetime(2015, 6, 24, tzinfo=tzinfo)
    assert test_datetime.date != datetime_not_expected


def test_datetime_member_with_timezone_with_miliseconds(db0_fixture):
    datetime_1 = datetime(2015, 6 , 24, 12, 30, 45, 999000, tzinfo=timezone.utc)
    datetime_expected = datetime(2015, 6, 24, 12, 30, 45, 999000, tzinfo=timezone.utc)
    test_datetime = DateTimeMock(datetime_1)
    assert test_datetime.date == datetime_expected


def test_convert_python_date_to_date_as_member(db0_fixture):
    date_1 = datetime(2015, 6, 24).date()
    date_expected = datetime(2015, 6, 24).date()
    test_date = DateTimeMock(date_1)
    assert test_date.date == date_expected


def test_date_member_returned_as_python_date(db0_fixture):
    date_1 = datetime(2015, 6, 24).date()
    date_expected = datetime(2015, 6, 24).date()
    test_date = DateTimeMock(date_1)
    assert test_date.date == date_expected
    test_date.date = test_date.date.replace(year=2016)
    date_expected = datetime(2016, 6, 24).date()
    assert test_date.date == date_expected



def test_convert_python_time_to_time_as_member(db0_fixture):
    time_1 = datetime(2015, 6, 24, 12, 30, 45).time()
    time_expected = datetime(2015, 6, 24, 12, 30, 45).time()
    test_time = DateTimeMock(time_1)
    assert test_time.date == time_expected


def test_time_member_returned_as_python_time(db0_fixture):
    time_1 = datetime(2015, 6, 24, 12, 30, 45).time()
    time_expected = datetime(2015, 6, 24, 12, 30, 45).time()
    test_time = DateTimeMock(time_1)
    assert test_time.date == time_expected
    test_time.date = test_time.date.replace(hour=14)
    time_expected = datetime(2015, 6, 24, 14, 30, 45).time()
    assert test_time.date == time_expected


def test_convert_python_time_to_time_as_member_with_timezone(db0_fixture):
    time_1 = datetime(2015, 6, 24, 12, 30, 45, tzinfo=timezone.utc).time()
    time_expected = datetime(2015, 6, 24, 12, 30, 45, tzinfo=timezone.utc).time()
    test_time = DateTimeMock(time_1)
    assert test_time.date == time_expected


def test_time_member_returned_as_python_time_with_timezone(db0_fixture):
    time_1 = datetime(2015, 6, 24, 12, 30, 45, tzinfo=timezone.utc).time()
    time_expected = datetime(2015, 6, 24, 12, 30, 45, tzinfo=timezone.utc).time()
    test_time = DateTimeMock(time_1)
    assert test_time.date == time_expected
    test_time.date = test_time.date.replace(hour=14)
    time_expected = datetime(2015, 6, 24, 14, 30, 45, tzinfo=timezone.utc).time()
    assert test_time.date == time_expected
