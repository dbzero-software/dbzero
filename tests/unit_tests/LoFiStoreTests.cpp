// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/object_model/object/lofi_store.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;
using namespace db0::object_model;
    
namespace tests

{
    
    class LoFiStoreTest: public testing::Test
    {
    public:
    };
    
    TEST_F( LoFiStoreTest, testLoFiStoreCreatedInitiallyEmpty )
    {
        lofi_store<2> cut;
        ASSERT_EQ(cut.size(), 21u);
        ASSERT_FALSE(cut.isFull());
    }

    TEST_F( LoFiStoreTest, testLoFiStoreAddSequentialUntilFull )
    {
        lofi_store<2> cut; // capacity 21
        for (unsigned int i = 0; i < cut.size(); ++i) {
            ASSERT_FALSE(cut.isFull() && i < cut.size() - 1) << "Store reported full too early at i=" << i;
            auto idx = cut.add(i & 0x3u); // value fits 2 bits
            ASSERT_EQ(idx, i);
            ASSERT_TRUE(cut.isSet(idx));
            ASSERT_EQ(cut.get(idx), (i & 0x3u));
        }
        ASSERT_TRUE(cut.isFull());
    }

    TEST_F( LoFiStoreTest, testLoFiStoreReinterpretCastFromValue )
    {
        std::uint64_t value = 0;
        lofi_store<2>::fromValue(value).set(0, 3);
        ASSERT_TRUE(reinterpret_cast<lofi_store<2>&>(value).isSet(0));
        ASSERT_EQ(reinterpret_cast<lofi_store<2>&>(value).get(0), 3u);

        lofi_store<2>::fromValue(value).set(1, 3);
        ASSERT_EQ(value, 0x3Fu); // both 0 and 1 set to 3
    }
    
    TEST_F( LoFiStoreTest, testLoFiStoreMask )
    {
        ASSERT_EQ(lofi_store<2>::mask(0), 0x7u);
        ASSERT_EQ(lofi_store<2>::mask(1), 0x38u);
        ASSERT_EQ(lofi_store<2>::mask(2), 0x1C0u);
    }
    
    TEST_F( LoFiStoreTest, testLoFiStoreReset )
    {
        std::uint64_t value = 0;
        lofi_store<2>::fromValue(value).set(0, 3);
        lofi_store<2>::fromValue(value).set(1, 3);
        lofi_store<2>::fromValue(value).set(7, 0);

        lofi_store<2>::fromValue(value).reset(1);
        ASSERT_FALSE(lofi_store<2>::fromValue(value).isSet(1));
        ASSERT_TRUE(lofi_store<2>::fromValue(value).isSet(0));
        ASSERT_TRUE(lofi_store<2>::fromValue(value).isSet(7));
    }
    
    TEST_F( LoFiStoreTest, testLoFiAllSlotsAllValues )
    {
        for (unsigned int i = 0; i < lofi_store<2>::size(); ++i) {
            for (unsigned int value = 0; value < 4; ++value) {
                std::uint64_t store_value = 0;
                lofi_store<2>::fromValue(store_value).set(i, value);
                ASSERT_TRUE(lofi_store<2>::fromValue(store_value).isSet(i));
                ASSERT_EQ(lofi_store<2>::fromValue(store_value).get(i), value);
            }
        }
    }
    
    TEST_F( LoFiStoreTest, testLoFiStoreIterator )
    {
        std::uint64_t value = 0;
        lofi_store<2>::fromValue(value).set(0, 3);
        lofi_store<2>::fromValue(value).set(1, 3);
        lofi_store<2>::fromValue(value).set(7, 0);
        lofi_store<2>::fromValue(value).set(13, 2);
        lofi_store<2>::fromValue(value).set(3, 1);

        std::vector<unsigned int> expected = { 3, 3, 1, 0, 2 };
        std::vector<unsigned int> expected_indices = { 0, 1, 3, 7, 13 };
        unsigned int count = 0;
        for (auto value: lofi_store<2>::fromValue(value)) {
            ASSERT_EQ(value, expected.front());
            expected.erase(expected.begin());
        }

        auto it = lofi_store<2>::fromValue(value).begin();
        for (;!it.isEnd(); ++it) {
            ASSERT_EQ(it.getOffset(), expected_indices.front());
            expected_indices.erase(expected_indices.begin());
            ++count;
        }
    }
    
    TEST_F( LoFiStoreTest, testLoFiIteratorIssue1 )
    {
        std::uint64_t value = 0x824;
        auto it = lofi_store<2>::fromValue(value).begin(), end = lofi_store<2>::fromValue(value).end();
        unsigned int count = 0;
        for (; it != end; ++it) {
            ++count;
        }
        ASSERT_EQ(count, 3u);
    }

}