// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <cstdlib>
#include <utility>

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/serialization/mu_store.hpp>
    
using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class MUStoreTest: public MemspaceTestBase
    {
    public:
        using MU_Store = v_object<db0::o_mu_store>;        
    };

    TEST_F( MUStoreTest , testMUStoreInstanceCreation )
    {        
        auto memspace = getMemspace();        
        ASSERT_NO_THROW( MU_Store(memspace, 256) );
    }

    TEST_F( MUStoreTest , testMUStoreSizeOfIsFixed )
    {        
        auto memspace = getMemspace();
        ASSERT_EQ( MU_Store(memspace, 256)->sizeOf(), 256 );
    }

    TEST_F( MUStoreTest , testMUStoreCanAppendAndIterateElements )
    {        
        auto memspace = getMemspace();
        MU_Store cut(memspace, 256);
        ASSERT_TRUE(cut.modify().tryAppend(0, 1));
        ASSERT_TRUE(cut.modify().tryAppend(10, 1));
        ASSERT_TRUE(cut.modify().tryAppend(127, 1));

        std::vector<std::pair<std::uint16_t, uint16_t> > items {
            {0, 1}, {10, 1}, {127, 1}
        };

        int index = 0;
        for (auto item: *cut.getData()) {
            ASSERT_EQ(item, items[index]);
            ++index;
        }
    }

    TEST_F( MUStoreTest , testMUStoreCompactRemovesConsecutiveDuplicates )
    {        
        auto memspace = getMemspace();
        MU_Store cut(memspace, 256);
        ASSERT_TRUE(cut.modify().tryAppend(10, 1));
        ASSERT_TRUE(cut.modify().tryAppend(10, 1));
        ASSERT_TRUE(cut.modify().tryAppend(127, 1));

        cut.modify().compact();
        // compaction should yield 2 elements only
        std::vector<std::pair<std::uint16_t, uint16_t> > items {
            {10, 1}, {127, 1}
        };

        int index = 0;
        for (auto item: *cut.getData()) {
            ASSERT_EQ(item, items[index]);
            ++index;
        }
    }

    TEST_F( MUStoreTest , testMUStoreCompactRemovesNonConsecutiveDuplicates )
    {
        auto memspace = getMemspace();
        MU_Store cut(memspace, 256);
        ASSERT_TRUE(cut.modify().tryAppend(10, 1));
        ASSERT_TRUE(cut.modify().tryAppend(127, 1));
        ASSERT_TRUE(cut.modify().tryAppend(10, 1));        

        cut.modify().compact();
        // compaction should yield 2 elements only
        std::vector<std::pair<std::uint16_t, uint16_t> > items {
            {10, 1}, {127, 1}
        };

        int index = 0;
        for (auto item: *cut.getData()) {
            ASSERT_EQ(item, items[index]);
            ++index;
        }
    }
    
    TEST_F( MUStoreTest , testMUStoreCompactMergesElements )
    {
        auto memspace = getMemspace();
        MU_Store cut(memspace, 256);
        ASSERT_TRUE(cut.modify().tryAppend(10, 6));
        ASSERT_TRUE(cut.modify().tryAppend(127, 1));
        ASSERT_TRUE(cut.modify().tryAppend(10, 17));

        cut.modify().compact();
        // compaction should yield 2 elements only
        std::vector<std::pair<std::uint16_t, uint16_t> > items {
            {10, 17}, {127, 1}
        };

        int index = 0;
        for (auto item: *cut.getData()) {
            ASSERT_EQ(item, items[index]);
            ++index;
        }
    }

    TEST_F( MUStoreTest , testMUStoreCompactMergesOverlappingElements )
    {
        auto memspace = getMemspace();
        MU_Store cut(memspace, 256);
        ASSERT_TRUE(cut.modify().tryAppend(10, 6));
        ASSERT_TRUE(cut.modify().tryAppend(127, 1));
        ASSERT_TRUE(cut.modify().tryAppend(14, 21));

        cut.modify().compact();
        // compaction should yield 2 elements only
        std::vector<std::pair<std::uint16_t, uint16_t> > items {
            {10, 25}, {127, 1}
        };

        int index = 0;
        for (auto item: *cut.getData()) {
            ASSERT_EQ(item, items[index]);
            ++index;
        }
    }

    TEST_F( MUStoreTest , testMUStoreCompactionNotPerformedAsLongAsCapacitySufficent )
    {
        auto memspace = getMemspace();
        MU_Store cut(memspace, 256);
        std::vector<std::pair<std::uint16_t, uint16_t> > items {
            {10, 6}, {127, 1}, {14, 21}
        };
        
        auto max_size = cut->maxSize();
        for (int i = 0; i < (int)max_size; ++i) {
            auto next_item = items[i % 3];
            ASSERT_TRUE(cut.modify().tryAppend(next_item.first, next_item.second));
        }
            
        ASSERT_EQ(cut->size(), max_size);
    }

    TEST_F( MUStoreTest , testMUStoreAutoCompactionAfterExceedingCapacity )
    {
        auto memspace = getMemspace();
        MU_Store cut(memspace, 256);
        std::vector<std::pair<std::uint16_t, uint16_t> > items {
            {10, 6}, {127, 1}, {14, 21}
        };
        
        auto max_size = cut->maxSize();
        for (int i = 0; i < (int)max_size + 1; ++i) {
            auto next_item = items[i % 3];
            cut.modify().tryAppend(next_item.first, next_item.second);
        }

        // size after compaction should go down to 2 (+1 appended element)
        ASSERT_EQ(cut->size(), 3u);
    }
    
    TEST_F( MUStoreTest , testMUStoreAppendFullRange )
    {
        auto memspace = getMemspace();
        MU_Store cut(memspace, 256);
        cut.modify().appendFullRange();
        ASSERT_FALSE(cut.modify().tryAppend(10, 6));
        ASSERT_EQ(cut->begin(), cut->end());
        ASSERT_EQ(cut->size(), 0);

        // after "clear" tryAppend should work normally
        cut.modify().clear();
        ASSERT_TRUE(cut.modify().tryAppend(10, 6));
        ASSERT_EQ(cut->size(), 1u);
    }

    TEST_F( MUStoreTest , testMUStoreMaxCapacity ) {
        ASSERT_TRUE( o_mu_store::maxCapacity() <= 768 );
    }
    
    TEST_F( MUStoreTest , testMUStoreClear )
    {
        auto memspace = getMemspace();
        MU_Store cut(memspace, 256);
        ASSERT_TRUE(cut.modify().tryAppend(10, 6));
        ASSERT_TRUE(cut.modify().tryAppend(127, 1));
        ASSERT_TRUE(cut.modify().tryAppend(10, 17));

        cut.modify().clear();
    }

}
