// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <set>
#include <vector>
#include <dbzero/core/collections/map/VInstanceMap.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/memory/VObjectCache.hpp>
#include <dbzero/core/serialization/Fixed.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/object_model/class/ClassFactory.hpp>
#include <dbzero/object_model/enum/EnumFactory.hpp>
#include <dbzero/object_model/tags/TagIndex.hpp>
#include <utils/TestBase.hpp>

namespace tests

{

    using namespace db0;

DB0_PACKED_BEGIN
    struct DB0_PACKED_ATTR o_v_instance_map_value: db0::o_fixed<o_v_instance_map_value>
    {
        std::uint32_t m_value = 0;
        std::uint32_t m_extra = 0;

        o_v_instance_map_value() = default;

        o_v_instance_map_value(std::uint32_t value, std::uint32_t extra)
            : m_value(value)
            , m_extra(extra)
        {
        }
    };
DB0_PACKED_END

    class VInstanceMapTest: public MemspaceTestBase
    {
    protected:
        VInstanceMapTest()
            : m_shared_object_list(1)
            , m_memspace(getMemspace())
            , m_cache(m_memspace, m_shared_object_list)
        {
        }

        FixedObjectList m_shared_object_list;
        Memspace m_memspace;
        VObjectCache m_cache;
    };

    TEST_F(VInstanceMapTest, testCanInsertAndLookupInstancesByNumericKey)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        auto inserted = cut.insert(42, 691, 17);

        auto found = cut.get(42);
        ASSERT_TRUE(found);
        ASSERT_EQ(inserted->getAddress(), found->getAddress());
        ASSERT_EQ(691u, (*found)->m_value);
        ASSERT_EQ(17u, (*found)->m_extra);
    }

    TEST_F(VInstanceMapTest, testKeysAreStoredInAscendingOrder)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        cut.insert(30, 3, 30);
        cut.insert(10, 1, 10);
        cut.insert(20, 2, 20);

        auto it = cut.begin();
        ASSERT_EQ(10u, (*it).key);
        ++it;
        ASSERT_EQ(20u, (*it).key);
        ++it;
        ASSERT_EQ(30u, (*it).key);
    }

    TEST_F(VInstanceMapTest, testEraseKeyRemovesTheMapping)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        cut.insert(1, 10, 1);
        cut.insert(2, 20, 2);

        ASSERT_TRUE(cut.erase(1));
        ASSERT_FALSE(cut.tryGet(1));
        ASSERT_TRUE(cut.tryGet(2));
        ASSERT_FALSE(cut.erase(3));
    }

    TEST_F(VInstanceMapTest, testEraseKeyDestroysUnderlyingInstance)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        Address value_address;
        {
            auto inserted = cut.insert(1, 10, 1);
            value_address = inserted->getAddress();
        }

        ASSERT_NO_THROW(m_memspace.getAllocator().getAllocSize(value_address));
        ASSERT_TRUE(cut.erase(1));
        ASSERT_FALSE(cut.tryGet(1));
        ASSERT_ANY_THROW(m_memspace.getAllocator().getAllocSize(value_address));
    }

    TEST_F(VInstanceMapTest, testReplacingExistingKeyDestroysOldInstance)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        auto first = cut.insert(1, 10, 1);
        auto first_address = first->getAddress();
        auto second = cut.insert(1, 20, 2);

        ASSERT_NE(first_address, second->getAddress());
        ASSERT_EQ(second->getAddress(), cut.get(1)->getAddress());
        ASSERT_ANY_THROW(m_memspace.getAllocator().getAllocSize(first_address));
    }

    TEST_F(VInstanceMapTest, testCanReopenInstanceAfterWeakCacheEntryExpired)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        {
            auto inserted = cut.insert(5, 81392, 99);
            ASSERT_EQ(81392u, (*inserted)->m_value);
        }

        auto reopened = cut.get(5);
        ASSERT_TRUE(reopened);
        ASSERT_EQ(81392u, (*reopened)->m_value);
    }

    TEST_F(VInstanceMapTest, testFindOrCreateReusesExistingValue)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        auto first = cut.findOrCreate(7, 100, 1);
        auto second = cut.findOrCreate(7, 200, 2);

        ASSERT_EQ(first->getAddress(), second->getAddress());
        ASSERT_EQ(100u, (*second)->m_value);
        ASSERT_EQ(1u, (*second)->m_extra);
    }

    TEST_F(VInstanceMapTest, testForAllVisitsActiveValuesEvenAfterVObjectCacheEviction)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        auto first = cut.insert(1, 10, 1);
        auto second = cut.insert(2, 20, 2);

        std::set<std::uint32_t> values;
        auto active_count = cut.forAll([&](ValueT &value) {
            values.insert(value->m_value);
        });

        ASSERT_EQ(2u, active_count);
        ASSERT_EQ(values, (std::set<std::uint32_t> { 10, 20 }));
        ASSERT_TRUE(first);
        ASSERT_TRUE(second);
    }

    TEST_F(VInstanceMapTest, testForAllCleansExpiredWeakValues)
    {
        using ValueT = db0::v_object<o_v_instance_map_value>;

        VInstanceMap<std::uint64_t, ValueT> cut(m_memspace, m_cache);
        {
            auto first = cut.insert(1, 10, 1);
            ASSERT_TRUE(first);
        }
        auto second = cut.insert(2, 20, 2);

        std::vector<std::uint32_t> values;
        auto active_count = cut.forAll([&](ValueT &value) {
            values.push_back(value->m_value);
        });

        ASSERT_EQ(1u, active_count);
        ASSERT_EQ(values, (std::vector<std::uint32_t> { 20 }));

        std::size_t second_pass_count = cut.forAll([](ValueT &) {});
        ASSERT_EQ(1u, second_pass_count);
        ASSERT_TRUE(second);
    }

    class VInstanceMapTagIndexTest: public FixtureTestBase
    {
    };

    TEST_F(VInstanceMapTagIndexTest, testCanStoreTagIndexInstances)
    {
        using TagIndex = db0::object_model::TagIndex;

        auto fixture = getFixture();
        db0::object_model::ClassFactory class_factory(fixture);
        db0::object_model::EnumFactory enum_factory(fixture);
        auto mutation_log = fixture->addMutationHandler();

        VInstanceMap<std::uint64_t, TagIndex> cut(*fixture, fixture->getVObjectCache());
        auto inserted = cut.insert(
            11,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        auto found = cut.get(
            11,
            class_factory,
            enum_factory,
            fixture->getLimitedStringPool(),
            fixture->getVObjectCache(),
            mutation_log
        );

        ASSERT_TRUE(found);
        ASSERT_EQ(inserted->getAddress(), found->getAddress());
        ASSERT_TRUE(found->empty());
    }

}
