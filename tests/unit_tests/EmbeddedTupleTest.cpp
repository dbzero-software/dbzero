// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <Python.h>
#include <datetime.h>
#include <dbzero/bindings/python/PySafeAPI.hpp>
#include <dbzero/bindings/python/shared_py_object.hpp>
#include <dbzero/bindings/python/types/DateTime.hpp>
#include <dbzero/bindings/python/types/PyDecimal.hpp>
#include <utils/TestBase.hpp>
#include <dbzero/core/serialization/bounded_buf_t.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/object_model/tuple/o_py_tuple.hpp>
#include <dbzero/object_model/tuple/o_tuple.hpp>

#include <stdexcept>

namespace tests
{

    using namespace db0;
    using namespace db0::object_model;
    using namespace db0::python;

    class EmbeddedTupleTest: public MemspaceTestBase
    {
    };

    static std::int64_t asInt64(const o_tuple_item &item)
    {
        if (item.itemKind() == TupleItemKind::PACKED_INT64) {
            return static_cast<std::int64_t>(item.packedIntPayload().value());
        }
        return item.intPayload().value();
    }

    static bool asBool(const o_tuple_item &item)
    {
        return item.boolPayload().value();
    }

    static double asDouble(const o_tuple_item &item)
    {
        return item.doublePayload().value();
    }

    static std::uint64_t asUint64(const o_tuple_item &item)
    {
        return item.uint64Payload().value();
    }

    static std::string asString(const o_tuple_item &item)
    {
        return item.stringPayload().toString();
    }

    static std::vector<std::byte> asBytes(const o_tuple_item &item)
    {
        const auto &payload = item.bytesPayload();
        return { payload.begin(), payload.end() };
    }

    static void throwDecodeError()
    {
        throw std::runtime_error("decode error");
    }

    static std::size_t measureElementBlock(const std::vector<o_tuple<>::Element> &elements)
    {
        std::size_t size = 0;
        for (const auto &element: elements) {
            size += o_tuple_item::measure(element);
        }
        return size;
    }

    TEST_F( EmbeddedTupleTest , testTupleStoresInlineAndVariableLengthElements )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = { std::byte{0x01}, std::byte{0x02}, std::byte{0xff} };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::integer(42),
            o_tuple<>::Element::string("alpha"),
            o_tuple<>::Element::boolean(true),
            o_tuple<>::Element::bytes(bytes)
        };

        v_object<o_tuple<> > tuple(memspace, elements);
        auto expectedElementsSize = measureElementBlock(elements);

        ASSERT_EQ(o_tuple<>::measure(elements),
            o_tuple<>::safeSizeOf(reinterpret_cast<const std::byte *>(tuple.getData())));
        ASSERT_EQ(tuple->size(), 4u);
        ASSERT_EQ(tuple->elementsByteSize(), expectedElementsSize);
        ASSERT_EQ(tuple->item(0).itemKind(), TupleItemKind::PACKED_INT64);
        ASSERT_EQ(asInt64(tuple->item(0)), 42);
        ASSERT_EQ(tuple->item(1).itemKind(), TupleItemKind::STRING);
        ASSERT_EQ(asString(tuple->item(1)), "alpha");
        ASSERT_EQ(tuple->item(2).itemKind(), TupleItemKind::BOOLEAN);
        ASSERT_TRUE(asBool(tuple->item(2)));
        ASSERT_EQ(tuple->item(3).itemKind(), TupleItemKind::BINARY);
        ASSERT_EQ(asBytes(tuple->item(3)), (std::vector<std::byte>{ std::byte{0x01}, std::byte{0x02}, std::byte{0xff} }));
    }

    TEST_F( EmbeddedTupleTest , testTupleItemKindValuesAreStable )
    {
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::UNDEFINED), 0u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::NONE), 1u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::BOOLEAN), 2u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::INT64), 3u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::FP_NUMERIC64), 4u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::STRING), 5u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::BINARY), 6u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::PTIME64), 7u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::DATE), 8u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::DATETIME), 9u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::DATETIME_TZ), 10u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::TIME), 11u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::TIME_TZ), 12u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::DECIMAL), 13u);
        ASSERT_EQ(static_cast<std::uint8_t>(TupleItemKind::PACKED_INT64), 14u);
    }

    TEST_F( EmbeddedTupleTest , testTupleUsesPackedInt64OnlyWhenItSavesAtLeastTwoBytes )
    {
        auto memspace = getMemspace();
        constexpr std::int64_t sixBytePackedMax = (std::int64_t{1} << 42) - 1;
        constexpr std::int64_t sevenBytePackedMin = std::int64_t{1} << 42;
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::integer(0),
            o_tuple<>::Element::integer(127),
            o_tuple<>::Element::integer(sixBytePackedMax),
            o_tuple<>::Element::integer(sevenBytePackedMin),
            o_tuple<>::Element::integer(-1)
        };

        v_object<o_tuple<> > tuple(memspace, elements);

        ASSERT_EQ(tuple->item(0).itemKind(), TupleItemKind::PACKED_INT64);
        ASSERT_EQ(asInt64(tuple->item(0)), 0);
        ASSERT_EQ(tuple->item(1).itemKind(), TupleItemKind::PACKED_INT64);
        ASSERT_EQ(asInt64(tuple->item(1)), 127);
        ASSERT_EQ(tuple->item(2).itemKind(), TupleItemKind::PACKED_INT64);
        ASSERT_EQ(asInt64(tuple->item(2)), sixBytePackedMax);
        ASSERT_EQ(tuple->item(3).itemKind(), TupleItemKind::INT64);
        ASSERT_EQ(asInt64(tuple->item(3)), sevenBytePackedMin);
        ASSERT_EQ(tuple->item(4).itemKind(), TupleItemKind::INT64);
        ASSERT_EQ(asInt64(tuple->item(4)), -1);

        ASSERT_EQ(tuple->item(0).sizeOf(), o_tuple_item::measure(elements[0]));
        ASSERT_EQ(tuple->item(3).sizeOf(), o_tuple_item::measure(elements[3]));
        ASSERT_LE(tuple->item(2).sizeOf() + 2, tuple->item(3).sizeOf());
    }

    TEST_F( EmbeddedTupleTest , testTupleStoresUint64ScalarKinds )
    {
        auto memspace = getMemspace();
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::timestamp(1001),
            o_tuple<>::Element::date(2002),
            o_tuple<>::Element::datetime(3003),
            o_tuple<>::Element::datetimeTz(4004),
            o_tuple<>::Element::time(5005),
            o_tuple<>::Element::timeTz(6006),
            o_tuple<>::Element::decimal(7007)
        };

        v_object<o_tuple<> > tuple(memspace, elements);

        ASSERT_EQ(tuple->size(), elements.size());
        ASSERT_EQ(tuple->item(0).itemKind(), TupleItemKind::PTIME64);
        ASSERT_EQ(asUint64(tuple->item(0)), 1001u);
        ASSERT_EQ(tuple->item(1).itemKind(), TupleItemKind::DATE);
        ASSERT_EQ(asUint64(tuple->item(1)), 2002u);
        ASSERT_EQ(tuple->item(2).itemKind(), TupleItemKind::DATETIME);
        ASSERT_EQ(asUint64(tuple->item(2)), 3003u);
        ASSERT_EQ(tuple->item(3).itemKind(), TupleItemKind::DATETIME_TZ);
        ASSERT_EQ(asUint64(tuple->item(3)), 4004u);
        ASSERT_EQ(tuple->item(4).itemKind(), TupleItemKind::TIME);
        ASSERT_EQ(asUint64(tuple->item(4)), 5005u);
        ASSERT_EQ(tuple->item(5).itemKind(), TupleItemKind::TIME_TZ);
        ASSERT_EQ(asUint64(tuple->item(5)), 6006u);
        ASSERT_EQ(tuple->item(6).itemKind(), TupleItemKind::DECIMAL);
        ASSERT_EQ(asUint64(tuple->item(6)), 7007u);
    }

    TEST_F( EmbeddedTupleTest , testTupleMeasureSizeOfAndSafeSizeOfWithMultipleVariableLengthElements )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> shortBytes = { std::byte{0x10}, std::byte{0x20} };
        const std::vector<std::byte> longBytes = {
            std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04},
            std::byte{0x05}, std::byte{0x06}, std::byte{0x07}, std::byte{0x08}
        };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::string(""),
            o_tuple<>::Element::bytes(shortBytes),
            o_tuple<>::Element::string("alpha"),
            o_tuple<>::Element::bytes(longBytes),
            o_tuple<>::Element::string("variable length string payload"),
            o_tuple<>::Element::integer(9001),
            o_tuple<>::Element::string("tail")
        };

        v_object<o_tuple<> > tuple(memspace, elements);
        auto *begin = reinterpret_cast<const std::byte *>(tuple.getData());
        auto expectedElementBytes = measureElementBlock(elements);
        auto expectedTotalBytes = o_tuple<>::measure(elements);

        ASSERT_EQ(tuple->size(), elements.size());
        ASSERT_EQ(tuple->elementsByteSize(), expectedElementBytes);
        ASSERT_EQ(tuple->sizeOf(), expectedTotalBytes);
        ASSERT_EQ(o_tuple<>::safeSizeOf(begin), expectedTotalBytes);
        ASSERT_EQ(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + expectedTotalBytes)),
            expectedTotalBytes);

        ASSERT_EQ(asString(tuple->item(0)), "");
        ASSERT_EQ(asBytes(tuple->item(1)), shortBytes);
        ASSERT_EQ(asString(tuple->item(2)), "alpha");
        ASSERT_EQ(asBytes(tuple->item(3)), longBytes);
        ASSERT_EQ(asString(tuple->item(4)), "variable length string payload");
        ASSERT_EQ(asInt64(tuple->item(5)), 9001);
        ASSERT_EQ(asString(tuple->item(6)), "tail");
    }

    TEST_F( EmbeddedTupleTest , testCompactTupleOmitsElementsByteSizeMember )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = { std::byte{0x10}, std::byte{0x20}, std::byte{0x30} };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::integer(11),
            o_tuple<>::Element::string("compact"),
            o_tuple<>::Element::bytes(bytes)
        };

        v_object<o_tuple<> > tuple(memspace, elements);
        v_object<o_compact_tuple> compactTuple(memspace, elements);

        ASSERT_EQ(compactTuple->size(), elements.size());
        ASSERT_EQ(compactTuple->elementsByteSize(), measureElementBlock(elements));
        ASSERT_EQ(compactTuple->sizeOf(), o_compact_tuple::measure(elements));
        ASSERT_EQ(o_compact_tuple::safeSizeOf(reinterpret_cast<const std::byte *>(compactTuple.getData())),
            compactTuple->sizeOf());
        ASSERT_LT(compactTuple->sizeOf(), tuple->sizeOf());
        ASSERT_EQ(asInt64(compactTuple->item(0)), 11);
        ASSERT_EQ(asString(compactTuple->item(1)), "compact");
        ASSERT_EQ(asBytes(compactTuple->item(2)), bytes);
    }

    TEST_F( EmbeddedTupleTest , testSafeSizeOfRejectsTruncatedMultipleVariableLengthTuple )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = {
            std::byte{0xaa}, std::byte{0xbb}, std::byte{0xcc}, std::byte{0xdd}, std::byte{0xee}
        };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::string("first variable payload"),
            o_tuple<>::Element::bytes(bytes),
            o_tuple<>::Element::string("second variable payload"),
            o_tuple<>::Element::bytes(bytes),
            o_tuple<>::Element::string("third variable payload")
        };
        v_object<o_tuple<> > tuple(memspace, elements);

        auto *begin = reinterpret_cast<const std::byte *>(tuple.getData());
        auto size = tuple->sizeOf();

        ASSERT_EQ(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size)), size);
        for (std::size_t truncatedSize = 0; truncatedSize < size; ++truncatedSize) {
            ASSERT_THROW(
                o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + truncatedSize)),
                std::runtime_error
            ) << "truncated size: " << truncatedSize;
        }
    }

    TEST_F( EmbeddedTupleTest , testTupleSurvivesReopen )
    {
        auto memspace = getMemspace();
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::none(),
            o_tuple<>::Element::string("variable length")
        };

        Address address;
        {
            v_object<o_tuple<> > tuple(memspace, elements);
            address = tuple.getAddress();
        }

        v_object<o_tuple<> > tuple(memspace.myPtr(address));

        ASSERT_EQ(tuple->sizeOf(), o_tuple<>::safeSizeOf(reinterpret_cast<const std::byte *>(tuple.getData())));
        ASSERT_EQ(tuple->size(), 2u);
        ASSERT_EQ(tuple->item(0).itemKind(), TupleItemKind::NONE);
        ASSERT_EQ(asString(tuple->item(1)), "variable length");
    }

    TEST_F( EmbeddedTupleTest , testSafeSizeOfValidatesBoundedBufferBeforeHeaderReads )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = { std::byte{0x01}, std::byte{0x02}, std::byte{0xff} };
        std::vector<o_tuple<>::Element> elements = {
            o_tuple<>::Element::integer(42),
            o_tuple<>::Element::bytes(bytes)
        };
        v_object<o_tuple<> > tuple(memspace, elements);

        auto *begin = reinterpret_cast<const std::byte *>(tuple.getData());
        auto size = tuple->sizeOf();

        ASSERT_EQ(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size)), size);
        ASSERT_THROW(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + 1)), std::runtime_error);
        ASSERT_THROW(o_tuple<>::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size - 1)), std::runtime_error);
    }

    TEST_F( EmbeddedTupleTest , testPyTupleConstructsFromPythonTuple )
    {
        Py_Initialize();
        auto memspace = getMemspace();
        auto pyTuple = Py_OWN(PyTuple_New(5));
        PySafeTuple_SetItem(*pyTuple, 0, Py_OWN(PyLong_FromLongLong(123)));
        PySafeTuple_SetItem(*pyTuple, 1, Py_OWN(PyUnicode_FromString("python")));
        PySafeTuple_SetItem(*pyTuple, 2, Py_OWN(PyBool_FromLong(1)));
        PySafeTuple_SetItem(*pyTuple, 3, Py_OWN(PyFloat_FromDouble(4.5)));
        PySafeTuple_SetItem(*pyTuple, 4, Py_OWN(PyBytes_FromStringAndSize("\x01\x02", 2)));

        v_object<o_py_tuple> tuple(memspace, *pyTuple);

        ASSERT_EQ(o_py_tuple::measure(*pyTuple), tuple->sizeOf());
        ASSERT_EQ(tuple->size(), 5u);
        ASSERT_EQ(tuple->item(0).itemKind(), TupleItemKind::PACKED_INT64);
        ASSERT_EQ(asInt64(tuple->item(0)), 123);
        ASSERT_EQ(tuple->item(1).itemKind(), TupleItemKind::STRING);
        ASSERT_EQ(asString(tuple->item(1)), "python");
        ASSERT_EQ(tuple->item(2).itemKind(), TupleItemKind::BOOLEAN);
        ASSERT_TRUE(asBool(tuple->item(2)));
        ASSERT_EQ(tuple->item(3).itemKind(), TupleItemKind::FP_NUMERIC64);
        ASSERT_EQ(asDouble(tuple->item(3)), 4.5);
        ASSERT_EQ(tuple->item(4).itemKind(), TupleItemKind::BINARY);
        ASSERT_EQ(asBytes(tuple->item(4)), (std::vector<std::byte>{ std::byte{0x01}, std::byte{0x02} }));
    }

    TEST_F( EmbeddedTupleTest , testPyTupleConstructsFromPythonList )
    {
        Py_Initialize();
        auto memspace = getMemspace();
        auto pyList = Py_OWN(PyList_New(2));
        PySafeList_SetItem(*pyList, 0, Py_OWN(Py_NewRef(Py_None)));
        PySafeList_SetItem(*pyList, 1, Py_OWN(PyUnicode_FromString("list item")));

        v_object<o_py_tuple> tuple(memspace, *pyList);

        ASSERT_EQ(o_py_tuple::measure(*pyList), tuple->sizeOf());
        ASSERT_EQ(tuple->size(), 2u);
        ASSERT_EQ(tuple->item(0).itemKind(), TupleItemKind::NONE);
        ASSERT_EQ(tuple->item(1).itemKind(), TupleItemKind::STRING);
        ASSERT_EQ(asString(tuple->item(1)), "list item");
    }

    TEST_F( EmbeddedTupleTest , testPyTupleMeasureSizeOfAndSafeSizeOfWithMultipleVariableLengthElements )
    {
        Py_Initialize();
        auto memspace = getMemspace();
        auto pyTuple = Py_OWN(PyTuple_New(6));
        PySafeTuple_SetItem(*pyTuple, 0, Py_OWN(PyUnicode_FromString("")));
        PySafeTuple_SetItem(*pyTuple, 1, Py_OWN(PyBytes_FromStringAndSize("abc", 3)));
        PySafeTuple_SetItem(*pyTuple, 2, Py_OWN(PyUnicode_FromString("middle variable string")));
        PySafeTuple_SetItem(*pyTuple, 3, Py_OWN(PyBytes_FromStringAndSize("0123456789", 10)));
        PySafeTuple_SetItem(*pyTuple, 4, Py_OWN(PyUnicode_FromString("tail variable string")));
        PySafeTuple_SetItem(*pyTuple, 5, Py_OWN(PyLong_FromLongLong(77)));

        v_object<o_py_tuple> tuple(memspace, *pyTuple);
        auto *begin = reinterpret_cast<const std::byte *>(tuple.getData());
        auto measured = o_py_tuple::measure(*pyTuple);

        ASSERT_EQ(tuple->size(), 6u);
        ASSERT_EQ(tuple->sizeOf(), measured);
        ASSERT_EQ(o_py_tuple::safeSizeOf(begin), measured);
        ASSERT_EQ(o_py_tuple::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + measured)), measured);
        ASSERT_EQ(asString(tuple->item(0)), "");
        ASSERT_EQ(asBytes(tuple->item(1)), (std::vector<std::byte>{ std::byte{'a'}, std::byte{'b'}, std::byte{'c'} }));
        ASSERT_EQ(asString(tuple->item(2)), "middle variable string");
        ASSERT_EQ(asBytes(tuple->item(3)), (std::vector<std::byte>{
            std::byte{'0'}, std::byte{'1'}, std::byte{'2'}, std::byte{'3'}, std::byte{'4'},
            std::byte{'5'}, std::byte{'6'}, std::byte{'7'}, std::byte{'8'}, std::byte{'9'}
        }));
        ASSERT_EQ(asString(tuple->item(4)), "tail variable string");
        ASSERT_EQ(asInt64(tuple->item(5)), 77);
    }

    TEST_F( EmbeddedTupleTest , testPyTupleConstructsFromDateTimeAndDecimal )
    {
        Py_Initialize();
        db0::python::init_datetime();
        if (!PyDateTimeAPI) {
            PyDateTime_IMPORT;
        }
        ASSERT_NE(PyDateTimeAPI, nullptr);
        auto memspace = getMemspace();
        auto pyTuple = Py_OWN(PyTuple_New(5));
        auto decimal = Py_OWN(PyObject_CallFunction(db0::python::getDecimalClass(), "s", "123.45"));
        ASSERT_NE(decimal.get(), nullptr);
        PySafeTuple_SetItem(*pyTuple, 0, Py_OWN(PyDate_FromDate(2026, 5, 19)));
        PySafeTuple_SetItem(*pyTuple, 1, Py_OWN(PyDateTime_FromDateAndTime(2026, 5, 19, 12, 34, 56, 789)));
        PySafeTuple_SetItem(*pyTuple, 2, Py_OWN(PyTime_FromTime(12, 34, 56, 789)));
        PySafeTuple_SetItem(*pyTuple, 3, Py_OWN(Py_NewRef(*decimal)));
        PySafeTuple_SetItem(*pyTuple, 4, Py_OWN(PyUnicode_FromString("tail")));

        v_object<o_py_tuple> tuple(memspace, *pyTuple);

        ASSERT_EQ(tuple->item(0).itemKind(), TupleItemKind::DATE);
        ASSERT_EQ(asUint64(tuple->item(0)), db0::python::pyDateToUint64(PyTuple_GET_ITEM(*pyTuple, 0)));
        ASSERT_EQ(tuple->item(1).itemKind(), TupleItemKind::DATETIME);
        ASSERT_EQ(asUint64(tuple->item(1)), db0::python::pyDateTimeToToUint64(PyTuple_GET_ITEM(*pyTuple, 1)));
        ASSERT_EQ(tuple->item(2).itemKind(), TupleItemKind::TIME);
        ASSERT_EQ(asUint64(tuple->item(2)), db0::python::pyTimeToUint64(PyTuple_GET_ITEM(*pyTuple, 2)));
        ASSERT_EQ(tuple->item(3).itemKind(), TupleItemKind::DECIMAL);
        ASSERT_EQ(asUint64(tuple->item(3)), db0::python::pyDecimalToUint64(PyTuple_GET_ITEM(*pyTuple, 3)));
        ASSERT_EQ(tuple->item(4).itemKind(), TupleItemKind::STRING);
        ASSERT_EQ(asString(tuple->item(4)), "tail");
    }

}
