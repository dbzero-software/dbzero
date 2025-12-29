// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/config.hpp>
#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/DP_Lock.hpp>
#include <dbzero/core/storage/Storage0.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    class PageMapTest: public testing::Test 
    {
    public:
        virtual void SetUp() override {            
        }

        virtual void TearDown() override {            
        }    
    };

    TEST_F( PageMapTest , testEmptyPageMap )
    {
        db0::Storage0 dev_null;
        PageMap<DP_Lock> cut(dev_null.getPageSize());
        StateNumType state_num;
        ASSERT_EQ(cut.find(1, 2, state_num), nullptr);
        ASSERT_EQ(cut.find(1, 10, state_num), nullptr);
        ASSERT_EQ(cut.find(7, 5, state_num), nullptr);        
    }
    
    TEST_F( PageMapTest , testPageMapCanFindClosestStateMatch )
    {
        db0::Storage0 dev_null;
        db0::DirtyCache null_cache(dev_null.getPageSize());
        db0::StorageContext null_context { null_cache, dev_null };
        auto page_size = dev_null.getPageSize();
        PageMap<DP_Lock> cut(dev_null.getPageSize());
        auto lock_1 = std::make_shared<DP_Lock>(null_context, page_size, 1, FlagSet<AccessOptions> {}, 0, 0);
        auto lock_2 = std::make_shared<DP_Lock>(null_context, page_size, 1, FlagSet<AccessOptions> {}, 0, 0);
        // state = 1, page_num = 1
        cut.insert(1, lock_1);
        // same page_num, different state
        cut.insert(11, lock_2);
        StateNumType state_num;
        ASSERT_NE(cut.find(1, 1, state_num), nullptr);
        ASSERT_EQ(cut.find(1, 1, state_num)->lock(), lock_1);
        ASSERT_NE(cut.find(7, 1, state_num), nullptr);
        ASSERT_EQ(cut.find(7, 1, state_num)->lock(), lock_1);
        ASSERT_NE(cut.find(16, 1, state_num), nullptr);
        ASSERT_EQ(cut.find(16, 1, state_num)->lock(), lock_2);
        ASSERT_EQ(cut.find(1, 2, state_num), nullptr);
    }
    
}
