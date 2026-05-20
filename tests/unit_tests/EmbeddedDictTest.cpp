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
#include <dbzero/object_model/dict/o_dict.hpp>
#include <dbzero/object_model/dict/o_py_dict.hpp>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <sstream>
#include <iomanip>
#include <unordered_set>
#include <vector>

namespace tests
{

    using namespace db0;
    using namespace db0::object_model;
    using namespace db0::python;

    class EmbeddedDictTest: public MemspaceTestBase
    {
    };

    static void throwDecodeError()
    {
        throw std::runtime_error("decode error");
    }

    static std::int64_t asInt64(const o_tuple_item &item)
    {
        if (item.itemKind() == StorageClass::PACKED_INT32) {
            return static_cast<std::int64_t>(item.packedIntPayload().value());
        }
        return item.intPayload().value();
    }

    static bool asBool(const o_tuple_item &item)
    {
        return item.boolPayload().value();
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

    static std::string bytesKey(const std::byte *data, std::size_t size)
    {
        std::ostringstream key;
        for (std::size_t i = 0; i < size; ++i) {
            key << std::hex << std::setw(2) << std::setfill('0') << static_cast<unsigned>(data[i]);
        }
        return key.str();
    }

    static std::string elementKey(const o_dict::Element &element)
    {
        std::ostringstream key;
        key << static_cast<unsigned>(element.m_kind) << ':';
        switch (element.m_kind) {
            case StorageClass::NONE:
                key << "none";
                break;
            case StorageClass::BOOLEAN:
                key << element.boolValue();
                break;
            case StorageClass::INT64:
            case StorageClass::PACKED_INT32:
                key << element.intValue();
                break;
            case StorageClass::FP_NUMERIC64:
                key << std::setprecision(17) << element.doubleValue();
                break;
            case StorageClass::STRING_REF:
                key << element.stringValue();
                break;
            case StorageClass::DB0_BYTES:
                key << bytesKey(element.bytesData(), element.bytesSize());
                break;
            case StorageClass::PTIME64:
            case StorageClass::DATE:
            case StorageClass::DATETIME:
            case StorageClass::DATETIME_TZ:
            case StorageClass::TIME:
            case StorageClass::TIME_TZ:
            case StorageClass::DECIMAL:
                key << element.uint64Value();
                break;
            default:
                throw std::runtime_error("unsupported test dict element kind");
        }
        return key.str();
    }

    static std::string itemKey(const o_tuple_item &item)
    {
        std::ostringstream key;
        key << static_cast<unsigned>(item.itemKind()) << ':';
        switch (item.itemKind()) {
            case StorageClass::NONE:
                key << "none";
                break;
            case StorageClass::BOOLEAN:
                key << item.boolPayload().value();
                break;
            case StorageClass::INT64:
                key << item.intPayload().value();
                break;
            case StorageClass::PACKED_INT32:
                key << item.packedIntPayload().value();
                break;
            case StorageClass::FP_NUMERIC64:
                key << std::setprecision(17) << item.doublePayload().value();
                break;
            case StorageClass::STRING_REF:
                key << item.stringPayload().toString();
                break;
            case StorageClass::DB0_BYTES:
                key << bytesKey(item.bytesPayload().begin(), item.bytesPayload().size());
                break;
            case StorageClass::PTIME64:
            case StorageClass::DATE:
            case StorageClass::DATETIME:
            case StorageClass::DATETIME_TZ:
            case StorageClass::TIME:
            case StorageClass::TIME_TZ:
            case StorageClass::DECIMAL:
                key << item.uint64Payload().value();
                break;
            default:
                throw std::runtime_error("unsupported test dict item kind");
        }
        return key.str();
    }

    static void assertItemEqualsElement(const o_tuple_item &item, const o_dict::Element &element)
    {
        ASSERT_EQ(itemKey(item), elementKey(element));
    }

    static std::size_t testHashIndexCapacity(std::size_t count)
    {
        if (count == 0) {
            return 0;
        }

        std::size_t capacity = 1;
        while (capacity < count * 2) {
            capacity <<= 1;
        }
        return capacity;
    }

    TEST_F( EmbeddedDictTest , testDictStoresSimpleKeyValuePairs )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = { std::byte{0x01}, std::byte{0x02}, std::byte{0x03} };
        o_dict::ElementMap elements;
        elements[o_dict::Element::integer(42)] = o_dict::Element::string("answer");
        elements[o_dict::Element::string("flag")] = o_dict::Element::boolean(true);
        elements[o_dict::Element::bytes(bytes)] = o_dict::Element::integer(-7);

        v_object<o_dict> dict(memspace, elements);

        ASSERT_EQ(dict->size(), 3u);
        ASSERT_FALSE(dict->empty());
        ASSERT_TRUE(dict->contains(o_dict::Element::integer(42)));
        ASSERT_TRUE(dict->contains(o_dict::Element::string("flag")));
        ASSERT_TRUE(dict->contains(o_dict::Element::bytes(bytes)));
        ASSERT_FALSE(dict->contains(o_dict::Element::string("missing")));
        ASSERT_EQ(asString(*dict->get(o_dict::Element::integer(42))), "answer");
        ASSERT_TRUE(asBool(*dict->get(o_dict::Element::string("flag"))));
        ASSERT_EQ(asInt64(*dict->get(o_dict::Element::bytes(bytes))), -7);
    }

    TEST_F( EmbeddedDictTest , testDictMapInputCollapsesDuplicateKeys )
    {
        auto memspace = getMemspace();
        o_dict::ElementMap elements;
        elements[o_dict::Element::integer(7)] = o_dict::Element::string("first");
        elements[o_dict::Element::integer(7)] = o_dict::Element::string("second");

        v_object<o_dict> dict(memspace, elements);

        ASSERT_EQ(dict->size(), 1u);
        ASSERT_EQ(asString(*dict->get(o_dict::Element::integer(7))), "second");
    }

    TEST_F( EmbeddedDictTest , testDictMeasureSizeOfAndSafeSizeOf )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = { std::byte{0xaa}, std::byte{0xbb} };
        o_dict::ElementMap elements;
        elements[o_dict::Element::string("alpha")] = o_dict::Element::string("variable length value");
        elements[o_dict::Element::integer(11)] = o_dict::Element::bytes(bytes);
        elements[o_dict::Element::date(20260519)] = o_dict::Element::decimal(123456789);

        v_object<o_dict> dict(memspace, elements);
        auto *begin = reinterpret_cast<const std::byte *>(dict.getData());
        auto measured = o_dict::measure(elements);

        ASSERT_EQ(dict->size(), 3u);
        ASSERT_EQ(dict->sizeOf(), measured);
        ASSERT_EQ(o_dict::safeSizeOf(begin), measured);
        ASSERT_EQ(o_dict::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + measured)), measured);
        ASSERT_EQ(asString(*dict->get(o_dict::Element::string("alpha"))), "variable length value");
        ASSERT_EQ(asBytes(*dict->get(o_dict::Element::integer(11))), bytes);
        ASSERT_EQ(dict->get(o_dict::Element::date(20260519))->uint64Payload().value(), 123456789u);
    }

    TEST_F( EmbeddedDictTest , testSafeSizeOfRejectsTruncatedDict )
    {
        auto memspace = getMemspace();
        o_dict::ElementMap elements;
        elements[o_dict::Element::string("first")] = o_dict::Element::string("value");
        elements[o_dict::Element::integer(99)] = o_dict::Element::integer(100);

        v_object<o_dict> dict(memspace, elements);
        auto *begin = reinterpret_cast<const std::byte *>(dict.getData());
        auto size = dict->sizeOf();

        ASSERT_EQ(o_dict::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size)), size);
        for (std::size_t truncatedSize = 0; truncatedSize < size; ++truncatedSize) {
            ASSERT_THROW(
                o_dict::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + truncatedSize)),
                std::runtime_error
            ) << "truncated size: " << truncatedSize;
        }
    }

    TEST_F( EmbeddedDictTest , testDictCollisionBucketsUseParallelKeyValueTuples )
    {
        auto memspace = getMemspace();
        constexpr std::size_t collisionCount = 16;
        auto capacity = testHashIndexCapacity(collisionCount);
        o_dict::ElementHash hash;
        auto collisionSlot = hash(o_dict::Element::integer(17)) % capacity;

        o_dict::ElementMap elements;
        for (std::int64_t candidate = 0; elements.size() < collisionCount; ++candidate) {
            auto key = o_dict::Element::integer(candidate);
            if (hash(key) % capacity == collisionSlot) {
                elements[key] = o_dict::Element::integer(candidate * 10);
            }
        }

        v_object<o_dict> dict(memspace, elements);

        ASSERT_EQ(dict->size(), collisionCount);
        ASSERT_EQ(dict->sizeOf(), o_dict::measure(elements));
        ASSERT_EQ(o_dict::safeSizeOf(reinterpret_cast<const std::byte *>(dict.getData())), dict->sizeOf());
        for (const auto &[key, value]: elements) {
            ASSERT_TRUE(dict->contains(key));
            ASSERT_EQ(asInt64(*dict->get(key)), value.intValue());
        }
        ASSERT_FALSE(dict->contains(o_dict::Element::integer(999999)));
    }

    TEST_F( EmbeddedDictTest , testDictIterationVisitsStoredPairs )
    {
        auto memspace = getMemspace();
        o_dict::ElementMap elements;
        elements[o_dict::Element::integer(1)] = o_dict::Element::string("one");
        elements[o_dict::Element::integer(2)] = o_dict::Element::string("two");
        elements[o_dict::Element::integer(3)] = o_dict::Element::string("three");

        v_object<o_dict> dict(memspace, elements);

        std::unordered_set<std::int64_t> keys;
        std::size_t count = 0;
        for (auto it = dict->begin(); it != dict->end(); ++it) {
            keys.insert(asInt64(it->key()));
            ASSERT_NE(it->value().itemKind(), StorageClass::UNDEFINED);
            ++count;
        }

        ASSERT_EQ(count, elements.size());
        ASSERT_EQ(keys.size(), elements.size());
        ASSERT_TRUE(keys.find(1) != keys.end());
        ASSERT_TRUE(keys.find(2) != keys.end());
        ASSERT_TRUE(keys.find(3) != keys.end());
    }

    TEST_F( EmbeddedDictTest , testComplexDictContainsAndIterationWithMixedTypes )
    {
        auto memspace = getMemspace();
        o_dict::ElementMap elements;
        std::vector<std::string> keyStrings;
        std::vector<std::string> valueStrings;
        std::vector<std::vector<std::byte>> keyBytes;
        std::vector<std::vector<std::byte>> valueBytes;
        keyStrings.reserve(80);
        valueStrings.reserve(80);
        keyBytes.reserve(40);
        valueBytes.reserve(40);

        elements[o_dict::Element::none()] = o_dict::Element::string("none-value");
        elements[o_dict::Element::boolean(false)] = o_dict::Element::integer(-1);
        elements[o_dict::Element::boolean(true)] = o_dict::Element::integer(1);

        for (std::int64_t i = 0; i < 60; ++i) {
            keyStrings.push_back("complex-key-int-value-" + std::to_string(i));
            elements[o_dict::Element::integer((i * 7919) - 50000)] = o_dict::Element::string(keyStrings.back());
        }
        for (std::size_t i = 0; i < 40; ++i) {
            keyStrings.push_back("complex-dict-key-" + std::to_string(i) + "-" + std::string(i % 13, 'k'));
            valueBytes.push_back({
                static_cast<std::byte>(i & 0xff),
                static_cast<std::byte>((i * 3) & 0xff),
                static_cast<std::byte>((i * 5) & 0xff),
                static_cast<std::byte>((i * 7) & 0xff)
            });
            elements[o_dict::Element::string(keyStrings.back())] = o_dict::Element::bytes(valueBytes.back());
        }
        for (std::size_t i = 0; i < 24; ++i) {
            keyBytes.push_back({
                static_cast<std::byte>((i + 1) & 0xff),
                static_cast<std::byte>((i * 11) & 0xff),
                static_cast<std::byte>((i * 17) & 0xff)
            });
            valueStrings.push_back("bytes-key-value-" + std::to_string(i));
            elements[o_dict::Element::bytes(keyBytes.back())] = o_dict::Element::string(valueStrings.back());
        }
        for (std::size_t i = 0; i < 16; ++i) {
            elements[o_dict::Element::floating(static_cast<double>(i) + 0.25)] =
                o_dict::Element::floating(static_cast<double>(i) + 0.75);
        }
        for (std::uint64_t i = 0; i < 4; ++i) {
            elements[o_dict::Element::timestamp(100000 + i)] = o_dict::Element::date(200000 + i);
            elements[o_dict::Element::date(300000 + i)] = o_dict::Element::datetime(400000 + i);
            elements[o_dict::Element::datetime(500000 + i)] = o_dict::Element::datetimeTz(600000 + i);
            elements[o_dict::Element::time(700000 + i)] = o_dict::Element::timeTz(800000 + i);
            elements[o_dict::Element::decimal(900000 + i)] = o_dict::Element::integer(static_cast<std::int64_t>(i));
        }

        constexpr std::size_t forcedCollisionCount = 24;
        o_dict::ElementHash hash;
        auto finalCapacity = testHashIndexCapacity(elements.size() + forcedCollisionCount);
        auto collisionSlot = hash(o_dict::Element::integer(17)) % finalCapacity;
        std::size_t foundCollisions = 0;
        for (std::int64_t candidate = 1000000; foundCollisions < forcedCollisionCount; ++candidate) {
            auto key = o_dict::Element::integer(candidate);
            if (hash(key) % finalCapacity != collisionSlot) {
                continue;
            }
            auto inserted = elements.emplace(key, o_dict::Element::integer(candidate * 2));
            if (inserted.second) {
                ++foundCollisions;
            }
        }
        ASSERT_GE(elements.size(), 100u);
        ASSERT_EQ(testHashIndexCapacity(elements.size()), finalCapacity);

        v_object<o_dict> dict(memspace, elements);

        ASSERT_EQ(dict->size(), elements.size());
        ASSERT_EQ(dict->sizeOf(), o_dict::measure(elements));
        ASSERT_EQ(o_dict::safeSizeOf(reinterpret_cast<const std::byte *>(dict.getData())), dict->sizeOf());

        std::unordered_set<std::string> expectedPairKeys;
        expectedPairKeys.reserve(elements.size());
        for (const auto &[key, value]: elements) {
            expectedPairKeys.insert(elementKey(key) + "=>" + elementKey(value));
            ASSERT_TRUE(dict->contains(key)) << elementKey(key);
            ASSERT_NE(dict->get(key), nullptr) << elementKey(key);
            assertItemEqualsElement(*dict->get(key), value);
        }
        ASSERT_FALSE(dict->contains(o_dict::Element::integer(999999999)));
        ASSERT_FALSE(dict->contains(o_dict::Element::string("complex-dict-missing")));

        std::unordered_set<std::string> iteratedPairKeys;
        iteratedPairKeys.reserve(dict->size());
        std::size_t iteratedCount = 0;
        for (auto it = dict->begin(); it != dict->end(); ++it) {
            auto pairKey = itemKey(it->key()) + "=>" + itemKey(it->value());
            ASSERT_TRUE(expectedPairKeys.find(pairKey) != expectedPairKeys.end()) << pairKey;
            ASSERT_TRUE(iteratedPairKeys.insert(pairKey).second) << pairKey;
            ++iteratedCount;
        }

        ASSERT_EQ(iteratedCount, expectedPairKeys.size());
        ASSERT_EQ(iteratedPairKeys, expectedPairKeys);
    }

    TEST_F( EmbeddedDictTest , testPyDictConstructsFromPythonDict )
    {
        Py_Initialize();
        auto memspace = getMemspace();
        auto pyDict = Py_OWN(PyDict_New());
        ASSERT_NE(pyDict.get(), nullptr);
        ASSERT_EQ(PySafeDict_SetItem(
            *pyDict, Py_OWN(PyLong_FromLongLong(42)), Py_OWN(PyUnicode_FromString("python-dict"))
        ), 0);
        ASSERT_EQ(PySafeDict_SetItem(
            *pyDict, Py_OWN(PyUnicode_FromString("flag")), Py_OWN(Py_NewRef(Py_True))
        ), 0);
        ASSERT_EQ(PySafeDict_SetItem(
            *pyDict, Py_OWN(PyBytes_FromStringAndSize("\x01\x02", 2)), Py_OWN(PyLong_FromLongLong(-7))
        ), 0);

        v_object<o_py_dict> dict(memspace, *pyDict);

        ASSERT_EQ(o_py_dict::measure(*pyDict), dict->sizeOf());
        ASSERT_EQ(dict->size(), 3u);
        ASSERT_EQ(asString(*dict->get(o_dict::Element::integer(42))), "python-dict");
        ASSERT_TRUE(asBool(*dict->get(o_dict::Element::string("flag"))));
        ASSERT_EQ(asInt64(*dict->get(o_dict::Element::bytes(
            std::vector<std::byte>{ std::byte{0x01}, std::byte{0x02} }
        ))), -7);
        ASSERT_FALSE(dict->contains(o_dict::Element::integer(99)));
    }

    TEST_F( EmbeddedDictTest , testPyDictConstructsFromDateTimeAndDecimal )
    {
        Py_Initialize();
        db0::python::init_datetime();
        if (!PyDateTimeAPI) {
            PyDateTime_IMPORT;
        }
        ASSERT_NE(PyDateTimeAPI, nullptr);
        auto memspace = getMemspace();
        auto pyDict = Py_OWN(PyDict_New());
        auto decimalKey = Py_OWN(PyObject_CallFunction(db0::python::getDecimalClass(), "s", "123.45"));
        auto decimalValue = Py_OWN(PyObject_CallFunction(db0::python::getDecimalClass(), "s", "987.65"));
        ASSERT_NE(decimalKey.get(), nullptr);
        ASSERT_NE(decimalValue.get(), nullptr);
        ASSERT_EQ(PySafeDict_SetItem(
            *pyDict, Py_OWN(PyDate_FromDate(2026, 5, 19)), Py_OWN(PyLong_FromLongLong(123))
        ), 0);
        ASSERT_EQ(PySafeDict_SetItem(
            *pyDict, Py_OWN(Py_NewRef(*decimalKey)), Py_OWN(Py_NewRef(*decimalValue))
        ), 0);

        v_object<o_py_dict> dict(memspace, *pyDict);
        auto dateKey = db0::python::pyDateToUint64(Py_OWN(PyDate_FromDate(2026, 5, 19)).get());
        auto decimalKeyValue = db0::python::pyDecimalToUint64(*decimalKey);
        auto decimalStoredValue = db0::python::pyDecimalToUint64(*decimalValue);

        ASSERT_EQ(dict->size(), 2u);
        ASSERT_EQ(asInt64(*dict->get(o_dict::Element::date(dateKey))), 123);
        ASSERT_EQ(dict->get(o_dict::Element::decimal(decimalKeyValue))->uint64Payload().value(), decimalStoredValue);
        ASSERT_EQ(o_py_dict::measure(*pyDict), dict->sizeOf());
    }

}
