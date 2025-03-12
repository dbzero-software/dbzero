from decimal import Decimal

from python_tests.test_object import DataClassWithAttr


def test_convert_python_decimal_to_decimal_as_member(db0_fixture):
    decimal = Decimal('2015.06')
    decimal_expected = Decimal('2015.06')
    test_decimal = DataClassWithAttr(decimal)
    assert test_decimal.value == decimal_expected

def test_convert_python_decimal_with_zero(db0_fixture):
    decimal = Decimal('0')
    decimal_expected = Decimal('0')
    test_decimal = DataClassWithAttr(decimal)
    assert test_decimal.value == decimal_expected

def test_convert_python_decimal_negative_numbers(db0_fixture):
    decimal = Decimal('-1.02')
    decimal_expected = Decimal('-1.02')
    test_decimal = DataClassWithAttr(decimal)
    print(test_decimal.value)
    assert test_decimal.value == decimal_expected

def test_convert_python_decimal_multiple_places(db0_fixture):
    print("test_convert_python_decimal_multiple_places 1")
    decimal = Decimal('0.123456789123456789123456789')
    decimal_expected = Decimal('0.12345678912345678')
    test_decimal = DataClassWithAttr(decimal)
    assert test_decimal.value == decimal_expected

def test_convert_python_decimal_max_values(db0_fixture):
    print("test_convert_python_decimal_multiple_places 1")
    decimal = Decimal('9.999999999999999999999999999')
    decimal_expected = Decimal('9.9999999999999999')
    test_decimal = DataClassWithAttr(decimal)
    assert test_decimal.value == decimal_expected
