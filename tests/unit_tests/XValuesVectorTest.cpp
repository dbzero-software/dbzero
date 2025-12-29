// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/object_model/object/XValuesVector.hpp>
#include <dbzero/object_model/value/Value.hpp>
#include <dbzero/object_model/value/XValue.hpp>
#include <dbzero/object_model/object/lofi_store.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;

namespace tests

{
    
    class XValuesVectorTest: public testing::Test
    {
    public:
        std::uint64_t getMask(unsigned int offset) {
            return lofi_store<2>::mask(offset);
        }
        
        Value valueAt(unsigned int offset, std::uint64_t value)
        {
            return lofi_store<2>::create(offset, value);         
        }
    };
    
    TEST_F( XValuesVectorTest, testSetGetRegularTypes )
    {   
        XValuesVector cut;
        cut.push_back({ 0, StorageClass::INT64, Value(123) });
        cut.push_back({ 12, StorageClass::DB0_BYTES, Value(912) });
        cut.push_back({ 3, StorageClass::DATETIME_TZ, Value(523) });

        std::pair<StorageClass, Value> val;
        ASSERT_TRUE(cut.tryGetAt(0, val));
        ASSERT_EQ(StorageClass::INT64, val.first);
        ASSERT_EQ(123, val.second.cast<std::uint64_t>());

        ASSERT_TRUE(cut.tryGetAt(3, val));
        ASSERT_EQ(StorageClass::DATETIME_TZ, val.first);
        ASSERT_EQ(523, val.second.cast<std::uint64_t>());

        ASSERT_TRUE(cut.tryGetAt(12, val));
        ASSERT_EQ(StorageClass::DB0_BYTES, val.first);
        ASSERT_EQ(912, val.second.cast<std::uint64_t>());
        
        ASSERT_FALSE(cut.tryGetAt(7, val));
        ASSERT_FALSE(cut.tryGetAt(1, val));
    }

    TEST_F( XValuesVectorTest, testSetGetMaskedTypes )
    {   
        XValuesVector cut;
        // use low 2 bits
        cut.push_back({ 0, StorageClass::PACK_2, valueAt(0, 1) }, getMask(0));
        // same index / use bits 2 - 4
        cut.push_back({ 0, StorageClass::PACK_2, valueAt(1, 1) }, getMask(1));

        std::pair<StorageClass, Value> val;
        ASSERT_TRUE(cut.tryGetAt(0, val));
        ASSERT_EQ(StorageClass::PACK_2, val.first);
        // both values merged
        ASSERT_EQ(val.second.cast<std::uint64_t>(), valueAt(0, 1).m_store | valueAt(1, 1).m_store);
    }

    TEST_F( XValuesVectorTest, testMaskedTypesAtMultipleLocations )
    {   
        XValuesVector cut;
        // use low 2 bits
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(0, 1) }, getMask(0));
        cut.push_back({ 0, StorageClass::INT64, Value(123) });
        cut.push_back({ 1, StorageClass::INT64, Value(345) });
        cut.push_back({ 4, StorageClass::PACK_2, valueAt(0, 0) }, getMask(0));
        // same index / use bits 2 - 4
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(1, 1) }, getMask(1));

        std::pair<StorageClass, Value> val;
        ASSERT_TRUE(cut.tryGetAt(3, val));
        ASSERT_EQ(StorageClass::PACK_2, val.first);
        // both values merged
        ASSERT_EQ(val.second.cast<std::uint64_t>(), valueAt(0, 1).m_store | valueAt(1, 1).m_store);
    }

    TEST_F( XValuesVectorTest, testOverwritingMaskedValues )
    {
        XValuesVector cut;
        // use low 2 bits
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(0, 1) }, getMask(0));
        cut.push_back({ 0, StorageClass::INT64, Value(123) });
        // same index / use bits 4 - 5
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(2, 1) }, getMask(2));
        cut.push_back({ 1, StorageClass::INT64, Value(345) });
        cut.push_back({ 4, StorageClass::PACK_2, valueAt(0, 0) }, getMask(0));
        // now, overwrite at index = 3 offset = 0
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(0, 0) }, getMask(0));

        std::pair<StorageClass, Value> val;
        ASSERT_TRUE(cut.tryGetAt(3, val));
        ASSERT_EQ(StorageClass::PACK_2, val.first);
        // both values merged
        ASSERT_EQ(val.second.cast<std::uint64_t>(), valueAt(0, 0).m_store | valueAt(2, 1).m_store);
    }

    TEST_F( XValuesVectorTest, testOverwritingMaskedValuesWithSort )
    {
        XValuesVector cut(4); // low sort threshold
        // use low 2 bits
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(0, 1) }, getMask(0));
        cut.push_back({ 0, StorageClass::INT64, Value(123) });
        // same index / use bits 4 - 5
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(2, 1) }, getMask(2));
        cut.push_back({ 1, StorageClass::INT64, Value(345) });
        // NOTE: shrink due to merging at index = 3
        ASSERT_EQ(cut.size(), 3);
        cut.push_back({ 4, StorageClass::PACK_2, valueAt(0, 0) }, getMask(0));
        // now, overwrite at index = 3 offset = 0
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(0, 0) }, getMask(0));
        ASSERT_EQ(cut.size(), 5);

        std::pair<StorageClass, Value> val;
        ASSERT_TRUE(cut.tryGetAt(3, val));
        ASSERT_EQ(StorageClass::PACK_2, val.first);
        // both values merged
        ASSERT_EQ(val.second.cast<std::uint64_t>(), valueAt(0, 0).m_store | valueAt(2, 1).m_store);
    }

    TEST_F( XValuesVectorTest, testDedupOnSort )
    {
        std::vector<unsigned int> sort_thresholds = { 4, 8, 10 };
        for (auto st: sort_thresholds) {
            XValuesVector cut(st); // custom sort threshold
            cut.push_back({ 3, StorageClass::PACK_2, valueAt(0, 1) }, getMask(0));
            cut.push_back({ 0, StorageClass::INT64, Value(143) });
            cut.push_back({ 1, StorageClass::INT64, Value(567) });
            cut.push_back({ 0, StorageClass::INT64, Value(2323) });
            cut.push_back({ 1, StorageClass::INT64, Value(567) });
            cut.push_back({ 0, StorageClass::INT64, Value(23) });
            cut.push_back({ 1, StorageClass::INT64, Value(237) });
            cut.push_back({ 0, StorageClass::INT64, Value(233) });
            cut.push_back({ 1, StorageClass::INT64, Value(567) });
            cut.push_back({ 2, StorageClass::INT64, Value(2234) });
            
            ASSERT_LE(cut.size(), 6u); // after dedup & merge
            std::pair<StorageClass, Value> val;
            ASSERT_TRUE(cut.tryGetAt(0, val));
            ASSERT_EQ(StorageClass::INT64, val.first);
            ASSERT_EQ(233, val.second.cast<std::uint64_t>());

            ASSERT_TRUE(cut.tryGetAt(1, val));
            ASSERT_EQ(StorageClass::INT64, val.first);
            ASSERT_EQ(567, val.second.cast<std::uint64_t>());

            ASSERT_TRUE(cut.tryGetAt(2, val));
            ASSERT_EQ(StorageClass::INT64, val.first);
            ASSERT_EQ(2234, val.second.cast<std::uint64_t>());

            ASSERT_TRUE(cut.tryGetAt(3, val));
            ASSERT_EQ(StorageClass::PACK_2, val.first);
            ASSERT_EQ(val.second.cast<std::uint64_t>(), valueAt(0, 1).m_store);

            ASSERT_FALSE(cut.tryGetAt(4, val));
            ASSERT_FALSE(cut.tryGetAt(5, val));        
        }
    }
    
    TEST_F( XValuesVectorTest, testRemove )
    {
        XValuesVector cut;
        // use low 2 bits
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(0, 1) }, getMask(0));
        cut.push_back({ 0, StorageClass::INT64, Value(123) });
        // same index / use bits 4 - 5
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(2, 1) }, getMask(2));
        cut.push_back({ 1, StorageClass::INT64, Value(345) });
        cut.push_back({ 4, StorageClass::PACK_2, valueAt(0, 0) }, getMask(0));
        // now, overwrite at index = 3 offset = 0
        cut.push_back({ 3, StorageClass::PACK_2, valueAt(0, 0) }, getMask(0));

        cut.remove(1);
        cut.remove(4, getMask(0));
        cut.remove(3, getMask(2));

        ASSERT_EQ(cut.size(), 2u);
        std::pair<StorageClass, Value> val;
        ASSERT_TRUE(cut.tryGetAt(0, val));
        ASSERT_EQ(StorageClass::INT64, val.first);
        ASSERT_EQ(123, val.second.cast<std::uint64_t>());

        ASSERT_FALSE(cut.tryGetAt(1, val));
        ASSERT_TRUE(cut.tryGetAt(3, val));
        ASSERT_EQ(StorageClass::PACK_2, val.first);
        ASSERT_EQ(val.second.cast<std::uint64_t>(), valueAt(0, 0).m_store);

        ASSERT_FALSE(cut.tryGetAt(4, val));    
    }
    
}