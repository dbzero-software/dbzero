from decimal import Decimal

from python_tests.test_object import DataClassWithAttr


def test_convert_python_decimal_to_decimal_as_member(db0_fixture):
    decimal = Decimal('2015.06')
    decimal_expected = Decimal('2015.06')
    test_decimal = DataClassWithAttr(decimal)
    assert test_decimal.value == decimal_expected