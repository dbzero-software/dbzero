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
#include <dbzero/core/utils/hash_func.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/object_model/set/o_py_set.hpp>
#include <dbzero/object_model/set/o_set.hpp>

#include <iomanip>
#include <sstream>
#include <string>
#include <stdexcept>
#include <unordered_set>
#include <vector>

namespace tests
{

    using namespace db0;
    using namespace db0::object_model;
    using namespace db0::python;

    class EmbeddedSetTest: public MemspaceTestBase
    {
    };

    static void throwDecodeError()
    {
        throw std::runtime_error("decode error");
    }

    static std::uint32_t testHashBytes(const void *data, std::size_t size, std::uint32_t seed)
    {
        static const std::byte empty = std::byte{0};
        auto hash = db0::murmurhash64A(size == 0 ? &empty : data, size, seed);
        return static_cast<std::uint32_t>(hash ^ (hash >> 32));
    }

    static std::uint32_t testElementHash(const o_set::Element &element)
    {
        auto seedKind = element.m_kind == StorageClass::PACKED_INT32 ? StorageClass::INT64 : element.m_kind;
        seedKind = seedKind == StorageClass::EMBEDDED_STRING ? StorageClass::STRING_REF : seedKind;
        seedKind = seedKind == StorageClass::EMBEDDED_BYTES ? StorageClass::DB0_BYTES : seedKind;
        auto seed = 0x9e3779b9U ^ static_cast<std::uint32_t>(seedKind);
        switch (element.m_kind) {
            case StorageClass::NONE:
                return testHashBytes(nullptr, 0, seed);
            case StorageClass::BOOLEAN: {
                auto value = element.boolValue();
                return testHashBytes(&value, sizeof(value), seed);
            }
            case StorageClass::INT64: {
                auto value = element.intValue();
                return testHashBytes(&value, sizeof(value), seed);
            }
            case StorageClass::PACKED_INT32: {
                auto value = element.intValue();
                return testHashBytes(&value, sizeof(value), seed);
            }
            case StorageClass::FP_NUMERIC64: {
                auto value = element.doubleValue();
                return testHashBytes(&value, sizeof(value), seed);
            }
            case StorageClass::STRING_REF:
            case StorageClass::EMBEDDED_STRING: {
                auto value = element.m_payload.m_string_value;
                return testHashBytes(value.data(), value.size(), seed);
            }
            case StorageClass::DB0_BYTES:
            case StorageClass::EMBEDDED_BYTES:
                return testHashBytes(element.bytesData(), element.bytesSize(), seed);
            case StorageClass::PTIME64:
            case StorageClass::DATE:
            case StorageClass::DATETIME:
            case StorageClass::DATETIME_TZ:
            case StorageClass::TIME:
            case StorageClass::TIME_TZ:
            case StorageClass::DECIMAL: {
                auto value = element.uint64Value();
                return testHashBytes(&value, sizeof(value), seed);
            }
            default:
                throw std::runtime_error("unsupported test set item storage class");
        }
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

    static std::string bytesKey(const std::byte *data, std::size_t size)
    {
        std::ostringstream key;
        for (std::size_t i = 0; i < size; ++i) {
            key << std::hex << std::setw(2) << std::setfill('0') << static_cast<unsigned>(data[i]);
        }
        return key.str();
    }

    static std::string elementKey(const o_set::Element &element)
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
            case StorageClass::EMBEDDED_STRING:
                key << element.stringValue();
                break;
            case StorageClass::DB0_BYTES:
            case StorageClass::EMBEDDED_BYTES:
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
                throw std::runtime_error("unsupported test set item storage class");
        }
        return key.str();
    }

    static std::string itemKey(const o_set::Item &item)
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
            case StorageClass::EMBEDDED_STRING:
                key << item.stringPayload().toString();
                break;
            case StorageClass::DB0_BYTES:
            case StorageClass::EMBEDDED_BYTES:
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
                throw std::runtime_error("unsupported test set item storage class");
        }
        return key.str();
    }

    static void assertSetSizeWithin(
        Memspace &memspace, const char *name, const o_set::ElementSet &elements, std::size_t maxSize
    )
    {
        auto measured = o_set::measure(elements);
        v_object<o_set> set(memspace, elements);

        ASSERT_EQ(set->size(), elements.size()) << name;
        ASSERT_EQ(set->sizeOf(), measured) << name;
        ASSERT_EQ(o_set::safeSizeOf(reinterpret_cast<const std::byte *>(set.getData())), measured) << name;
        ASSERT_LE(measured, maxSize) << name;
    }

    TEST_F( EmbeddedSetTest , testSetStoresUniqueSimpleItems )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = { std::byte{0x01}, std::byte{0x02}, std::byte{0x03} };
        o_set::ElementSet elements = {
            o_set::Element::integer(42),
            o_set::Element::string("alpha"),
            o_set::Element::boolean(true),
            o_set::Element::bytes(bytes),
            o_set::Element::integer(42),
            o_set::Element::string("alpha"),
            o_set::Element::boolean(true),
            o_set::Element::bytes(bytes)
        };

        v_object<o_set> set(memspace, elements);

        ASSERT_EQ(set->size(), 4u);
        ASSERT_FALSE(set->empty());
        ASSERT_TRUE(set->contains(o_set::Element::integer(42)));
        ASSERT_TRUE(set->contains(o_set::Element::string("alpha")));
        ASSERT_TRUE(set->contains(o_set::Element::boolean(true)));
        ASSERT_TRUE(set->contains(o_set::Element::bytes(bytes)));
        ASSERT_FALSE(set->contains(o_set::Element::integer(100)));
        ASSERT_FALSE(set->contains(o_set::Element::string("missing")));
    }

    TEST_F( EmbeddedSetTest , testSetMeasureSizeOfAndSafeSizeOf )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> shortBytes = { std::byte{0x10}, std::byte{0x20} };
        const std::vector<std::byte> longBytes = {
            std::byte{0xaa}, std::byte{0xbb}, std::byte{0xcc}, std::byte{0xdd},
            std::byte{0xee}, std::byte{0xff}
        };
        o_set::ElementSet elements = {
            o_set::Element::string(""),
            o_set::Element::bytes(shortBytes),
            o_set::Element::string("set variable string"),
            o_set::Element::bytes(longBytes),
            o_set::Element::integer(7),
            o_set::Element::string("set variable string"),
            o_set::Element::bytes(longBytes)
        };

        v_object<o_set> set(memspace, elements);
        auto *begin = reinterpret_cast<const std::byte *>(set.getData());
        auto measured = o_set::measure(elements);

        ASSERT_EQ(set->size(), 5u);
        ASSERT_EQ(set->sizeOf(), measured);
        ASSERT_EQ(o_set::safeSizeOf(begin), measured);
        ASSERT_EQ(o_set::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + measured)), measured);
        ASSERT_GT(set->sizeOf(), 0u);
        ASSERT_TRUE(set->contains(o_set::Element::string("")));
        ASSERT_TRUE(set->contains(o_set::Element::bytes(shortBytes)));
        ASSERT_TRUE(set->contains(o_set::Element::string("set variable string")));
        ASSERT_TRUE(set->contains(o_set::Element::bytes(longBytes)));
        ASSERT_TRUE(set->contains(o_set::Element::integer(7)));
    }

    TEST_F( EmbeddedSetTest , testSetMeasuredSizeDoesNotRegress )
    {
        auto memspace = getMemspace();

        // Python comparison values below were measured on CPython 3.11 x86_64 with sys.getsizeof.
        // "set" is the shallow hash-table object size; "total" recursively includes contained
        // Python objects. Allocator arena/pool overhead is not included, so real process footprint is higher.
        // o_set max bytes vs Python set/total bytes:
        // empty: 3 vs 216/216
        assertSetSizeWithin(memspace, "empty", {}, 3u);
        // singleton packed integer: 13 vs 216/244
        assertSetSizeWithin(memspace, "singleton packed integer", { o_set::Element::integer(7) }, 13u);
        // singleton fixed integer: 20 vs 216/244
        assertSetSizeWithin(memspace, "singleton fixed integer", { o_set::Element::integer(-7) }, 20u);
        // mixed small scalar values: 54 vs 216/312
        assertSetSizeWithin(memspace, "mixed small", {
            o_set::Element::none(),
            o_set::Element::boolean(true),
            o_set::Element::integer(42),
            o_set::Element::floating(1.25)
        }, 54u);

        const std::vector<std::byte> shortBytes = { std::byte{0x01}, std::byte{0x02}, std::byte{0x03} };
        const std::vector<std::byte> longBytes = {
            std::byte{0x00}, std::byte{0x01}, std::byte{0x02}, std::byte{0x03},
            std::byte{0x04}, std::byte{0x05}, std::byte{0x06}, std::byte{0x07},
            std::byte{0x08}, std::byte{0x09}, std::byte{0x0a}, std::byte{0x0b},
            std::byte{0x0c}, std::byte{0x0d}, std::byte{0x0e}, std::byte{0x0f}
        };
        // variable-length strings and bytes: 166 vs 472/750
        assertSetSizeWithin(memspace, "variable length", {
            o_set::Element::string(""),
            o_set::Element::string("short"),
            o_set::Element::string("a somewhat longer string"),
            o_set::Element::bytes(shortBytes),
            o_set::Element::bytes(longBytes)
        }, 166u);

        // date/datetime/decimal scalar payloads: 62 vs 216/400
        assertSetSizeWithin(memspace, "uint64 scalar kinds", {
            o_set::Element::date(20260519),
            o_set::Element::datetime(123456789),
            o_set::Element::decimal(987654321)
        }, 62u);

        constexpr std::size_t collisionCount = 16;
        auto collisionCapacity = testHashIndexCapacity(collisionCount);
        auto collisionSlot = testElementHash(o_set::Element::integer(17)) % collisionCapacity;
        o_set::ElementSet collisions;
        for (std::int64_t candidate = 0; collisions.size() < collisionCount; ++candidate) {
            auto element = o_set::Element::integer(candidate);
            if (testElementHash(element) % collisionCapacity == collisionSlot) {
                collisions.insert(element);
            }
        }
        // 16 forced integer collisions: 222 vs 728/1176 for a Python set of 16 ints.
        assertSetSizeWithin(memspace, "forced collisions", collisions, 222u);

        o_set::ElementSet large;
        std::vector<std::string> strings;
        strings.reserve(64);
        for (std::int64_t i = 0; i < 64; ++i) {
            large.insert(o_set::Element::integer(i));
            strings.push_back("item-" + std::to_string(i));
            large.insert(o_set::Element::string(strings.back()));
        }
        // 64 ints plus 64 strings: 2050 vs 4312/9816
        assertSetSizeWithin(memspace, "large mixed", large, 2050u);
    }

    TEST_F( EmbeddedSetTest , testSingletonHashSlotDoesNotAllocateBucketTuple )
    {
        auto memspace = getMemspace();
        o_set::ElementSet elements = { o_set::Element::integer(7) };
        std::vector<o_tuple<>::Element> bucketElements = { o_tuple<>::Element::integer(7) };

        v_object<o_set> set(memspace, elements);

        ASSERT_EQ(set->size(), 1u);
        ASSERT_TRUE(set->contains(o_set::Element::integer(7)));
        ASSERT_FALSE(set->contains(o_set::Element::integer(8)));
        ASSERT_LT(set->sizeOf(), o_set::measure(elements) + o_tuple<>::measure(bucketElements));
    }

    TEST_F( EmbeddedSetTest , testSafeSizeOfRejectsTruncatedSet )
    {
        auto memspace = getMemspace();
        const std::vector<std::byte> bytes = {
            std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04}
        };
        o_set::ElementSet elements = {
            o_set::Element::string("first"),
            o_set::Element::bytes(bytes),
            o_set::Element::string("second")
        };
        v_object<o_set> set(memspace, elements);

        auto *begin = reinterpret_cast<const std::byte *>(set.getData());
        auto size = set->sizeOf();

        ASSERT_EQ(o_set::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size)), size);
        for (std::size_t truncatedSize = 0; truncatedSize < size; ++truncatedSize) {
            ASSERT_THROW(
                o_set::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + truncatedSize)),
                std::runtime_error
            ) << "truncated size: " << truncatedSize;
        }
    }

    TEST_F( EmbeddedSetTest , testSetSurvivesReopen )
    {
        auto memspace = getMemspace();
        o_set::ElementSet elements = {
            o_set::Element::none(),
            o_set::Element::integer(5),
            o_set::Element::string("reopened")
        };

        Address address;
        {
            v_object<o_set> set(memspace, elements);
            address = set.getAddress();
        }

        v_object<o_set> set(memspace.myPtr(address));

        ASSERT_EQ(set->size(), 3u);
        ASSERT_TRUE(set->contains(o_set::Element::none()));
        ASSERT_TRUE(set->contains(o_set::Element::integer(5)));
        ASSERT_TRUE(set->contains(o_set::Element::string("reopened")));
        ASSERT_EQ(set->sizeOf(), o_set::safeSizeOf(reinterpret_cast<const std::byte *>(set.getData())));
    }

    TEST_F( EmbeddedSetTest , testLargeSetMembershipUsesEmbeddedHashIndex )
    {
        auto memspace = getMemspace();
        o_set::ElementSet elements;
        std::vector<std::string> strings;
        strings.reserve(256);

        for (std::int64_t i = 0; i < 256; ++i) {
            elements.insert(o_set::Element::integer(i));
            strings.push_back("set-item-" + std::to_string(i));
            elements.insert(o_set::Element::string(strings.back()));
        }
        elements.insert(o_set::Element::integer(17));
        elements.insert(o_set::Element::string(strings[21]));

        v_object<o_set> set(memspace, elements);
        auto measured = o_set::measure(elements);

        ASSERT_EQ(set->size(), 512u);
        ASSERT_EQ(set->sizeOf(), measured);
        ASSERT_GT(set->sizeOf(), elements.size());
        ASSERT_TRUE(set->contains(o_set::Element::integer(0)));
        ASSERT_TRUE(set->contains(o_set::Element::integer(255)));
        ASSERT_TRUE(set->contains(o_set::Element::string(strings[0])));
        ASSERT_TRUE(set->contains(o_set::Element::string(strings[255])));
        ASSERT_FALSE(set->contains(o_set::Element::integer(512)));
        ASSERT_FALSE(set->contains(o_set::Element::string("set-item-missing")));
    }

    TEST_F( EmbeddedSetTest , testComplexSetContainsAndIterationWithForcedCollisions )
    {
        auto memspace = getMemspace();
        o_set::ElementSet elements;
        std::vector<std::string> strings;
        std::vector<std::vector<std::byte>> byteValues;
        strings.reserve(220);
        byteValues.reserve(100);

        elements.insert(o_set::Element::none());
        elements.insert(o_set::Element::boolean(false));
        elements.insert(o_set::Element::boolean(true));

        for (std::int64_t i = 0; i < 180; ++i) {
            elements.insert(o_set::Element::integer((i * 7919) - 50000));
        }
        for (std::size_t i = 0; i < 100; ++i) {
            strings.push_back("complex-set-string-" + std::to_string(i) + "-" + std::string(i % 17, 'x'));
            elements.insert(o_set::Element::string(strings.back()));
        }
        for (std::size_t i = 0; i < 80; ++i) {
            byteValues.push_back({
                static_cast<std::byte>(i & 0xff),
                static_cast<std::byte>((i * 3) & 0xff),
                static_cast<std::byte>((i * 7) & 0xff),
                static_cast<std::byte>((i * 11) & 0xff),
                static_cast<std::byte>((i * 13) & 0xff)
            });
            elements.insert(o_set::Element::bytes(byteValues.back()));
        }
        for (std::size_t i = 0; i < 40; ++i) {
            elements.insert(o_set::Element::floating(static_cast<double>(i) + 0.125));
        }
        for (std::uint64_t i = 0; i < 10; ++i) {
            elements.insert(o_set::Element::timestamp(100000 + i));
            elements.insert(o_set::Element::date(200000 + i));
            elements.insert(o_set::Element::datetime(300000 + i));
            elements.insert(o_set::Element::datetimeTz(400000 + i));
            elements.insert(o_set::Element::time(500000 + i));
            elements.insert(o_set::Element::timeTz(600000 + i));
            elements.insert(o_set::Element::decimal(700000 + i));
        }

        constexpr std::size_t forcedCollisionCount = 32;
        auto finalCapacity = testHashIndexCapacity(elements.size() + forcedCollisionCount);
        auto collisionSlot = testElementHash(o_set::Element::integer(17)) % finalCapacity;
        std::size_t foundCollisions = 0;
        for (std::int64_t candidate = 1000000; foundCollisions < forcedCollisionCount; ++candidate) {
            auto element = o_set::Element::integer(candidate);
            if (testElementHash(element) % finalCapacity != collisionSlot) {
                continue;
            }
            auto inserted = elements.insert(element);
            if (inserted.second) {
                ++foundCollisions;
            }
        }
        ASSERT_EQ(testHashIndexCapacity(elements.size()), finalCapacity);

        std::unordered_set<std::string> expectedKeys;
        expectedKeys.reserve(elements.size());
        for (const auto &element: elements) {
            expectedKeys.insert(elementKey(element));
        }

        v_object<o_set> set(memspace, elements);

        ASSERT_EQ(set->size(), elements.size());
        ASSERT_EQ(set->sizeOf(), o_set::measure(elements));
        ASSERT_EQ(o_set::safeSizeOf(reinterpret_cast<const std::byte *>(set.getData())), set->sizeOf());

        for (const auto &element: elements) {
            ASSERT_TRUE(set->contains(element)) << elementKey(element);
        }
        ASSERT_FALSE(set->contains(o_set::Element::integer(999999999)));
        ASSERT_FALSE(set->contains(o_set::Element::string("complex-set-string-missing")));

        std::unordered_set<std::string> iteratedKeys;
        iteratedKeys.reserve(set->size());
        std::size_t iteratedCount = 0;
        const auto &setRef = set.const_ref();
        for (auto it = setRef.begin(); it != setRef.end(); ++it) {
            const auto &item = *it;
            auto key = itemKey(item);
            ASSERT_TRUE(expectedKeys.find(key) != expectedKeys.end()) << key;
            ASSERT_TRUE(iteratedKeys.insert(key).second) << key;
            ++iteratedCount;
        }

        ASSERT_EQ(iteratedCount, expectedKeys.size());
        ASSERT_EQ(iteratedKeys, expectedKeys);
    }

    TEST_F( EmbeddedSetTest , testPySetConstructsFromPythonSet )
    {
        Py_Initialize();
        auto memspace = getMemspace();
        auto pySet = Py_OWN(PySet_New(nullptr));
        ASSERT_NE(pySet.get(), nullptr);
        ASSERT_EQ(PySafeSet_Add(*pySet, Py_OWN(PyLong_FromLongLong(42))), 0);
        ASSERT_EQ(PySafeSet_Add(*pySet, Py_OWN(PyUnicode_FromString("python-set"))), 0);
        ASSERT_EQ(PySafeSet_Add(*pySet, Py_OWN(Py_NewRef(Py_True))), 0);
        ASSERT_EQ(PySafeSet_Add(*pySet, Py_OWN(PyBytes_FromStringAndSize("\x01\x02", 2))), 0);

        v_object<o_py_set> set(memspace, *pySet);

        ASSERT_EQ(o_py_set::measure(*pySet), set->sizeOf());
        ASSERT_EQ(set->size(), 4u);
        ASSERT_TRUE(set->contains(o_set::Element::integer(42)));
        ASSERT_TRUE(set->contains(o_set::Element::string("python-set")));
        ASSERT_TRUE(set->contains(o_set::Element::boolean(true)));
        ASSERT_TRUE(set->contains(o_set::Element::bytes(
            std::vector<std::byte>{ std::byte{0x01}, std::byte{0x02} }
        )));
        ASSERT_FALSE(set->contains(o_set::Element::integer(99)));
    }

    TEST_F( EmbeddedSetTest , testPySetConstructsFromDateTimeAndDecimal )
    {
        Py_Initialize();
        db0::python::init_datetime();
        if (!PyDateTimeAPI) {
            PyDateTime_IMPORT;
        }
        ASSERT_NE(PyDateTimeAPI, nullptr);
        auto memspace = getMemspace();
        auto pySet = Py_OWN(PySet_New(nullptr));
        auto decimal = Py_OWN(PyObject_CallFunction(db0::python::getDecimalClass(), "s", "123.45"));
        ASSERT_NE(decimal.get(), nullptr);
        ASSERT_EQ(PySafeSet_Add(*pySet, Py_OWN(PyDate_FromDate(2026, 5, 19))), 0);
        ASSERT_EQ(PySafeSet_Add(*pySet, Py_OWN(Py_NewRef(*decimal))), 0);
        ASSERT_EQ(PySafeSet_Add(*pySet, Py_OWN(PyLong_FromLongLong(123))), 0);
        ASSERT_EQ(PySafeSet_Add(*pySet, Py_OWN(PyLong_FromLongLong(123))), 0);

        v_object<o_py_set> set(memspace, *pySet);
        auto dateValue = db0::python::pyDateToUint64(Py_OWN(PyDate_FromDate(2026, 5, 19)).get());
        auto decimalValue = db0::python::pyDecimalToUint64(*decimal);

        ASSERT_EQ(set->size(), 3u);
        ASSERT_TRUE(set->contains(o_set::Element::date(dateValue)));
        ASSERT_TRUE(set->contains(o_set::Element::decimal(decimalValue)));
        ASSERT_TRUE(set->contains(o_set::Element::integer(123)));

        std::unordered_set<std::string> iteratedKeys;
        for (auto it = set->begin(); it != set->end(); ++it) {
            iteratedKeys.insert(itemKey(*it));
        }
        ASSERT_EQ(iteratedKeys.size(), 3u);
    }

}
