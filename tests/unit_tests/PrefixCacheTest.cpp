// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/DP_Lock.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/storage/Storage0.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    class PrefixCacheTest: public testing::Test
    {
    public:
        void SetUp() override {            
        }

        void TearDown() override {            
        }    
    };
    
    TEST_F( PrefixCacheTest , testPrefixCacheCanTrackDPNegations )
    {
        db0::Storage0 dev_null;
        PrefixCache cut(dev_null, nullptr, 0);

        // state = 1, page_num = 0
        auto lock_1 = cut.createPage(0, 0, 1, { AccessOptions::write });
        // same page, different state (11)
        auto lock_2 = cut.createPage(0, 0, 11, { AccessOptions::write });
        
        StateNumType state_num;
        ASSERT_EQ(cut.findPage(0, 1, {}, state_num), lock_1);
        // state 14 is reported as existing
        ASSERT_NE(cut.findPage(0, 14, {}, state_num), nullptr);
        
        // add range as negated in state 12
        cut.markAsMissing(0, 12);
        // state not existing in cache
        ASSERT_EQ(cut.findPage(0, 14, {}, state_num), nullptr);
        cut.release();
    }
    
    TEST_F( PrefixCacheTest , testPrefixCacheNegationsClearedByCreatePage )
    {
        db0::Storage0 dev_null;
        PrefixCache cut(dev_null, nullptr, 0);

        // address, state_num, size
        auto lock_1 = cut.createPage(0, 0, 1, { AccessOptions::write });
        auto lock_2 = cut.createPage(0, 0, 11, { AccessOptions::write });
        // this simulates transaction 12 on page #0 executed by an external process
        cut.markAsMissing(0, 12);
        // this simulates retrieval of data from state_num = 12
        auto lock_3 = cut.createPage(0, 0, 12, { AccessOptions::write });
        
        StateNumType state_num;
        ASSERT_EQ(cut.findPage(0, 14, {}, state_num), lock_3);
        cut.release();
    }
    
    TEST_F( PrefixCacheTest , testPrefixCacheUpdateStateNumToAvoidCoW )
    {
        db0::Storage0 dev_null;
        std::atomic<std::size_t> null_meter = 0;
        db0::CacheRecycler cache_recycler(1 << 20u, null_meter);
        PrefixCache cut(dev_null, &cache_recycler, 0);

        // page num, read state num, state num
        // create page in state #1
        {
            auto lock = cut.createPage(0, 0, 1, { AccessOptions::write });
            cut.commit();
        }
        // request state #2 for read-write (note that lock has been released)
        StateNumType read_state_num;
        auto lock = cut.findPage(0, 2, { AccessOptions::read, AccessOptions::write }, read_state_num);
        ASSERT_TRUE(lock);
        
        // make sure only 1 lock is cached (no CoW), since upgrade took place
        ASSERT_EQ(cache_recycler.size(), lock->usedMem());
        
        // now, try reading the state #1 version of the page
        auto lock_1 = cut.findPage(0, 1, { AccessOptions::read }, read_state_num);
        // lock of this version should be no longer present in cache
        ASSERT_FALSE(lock_1);
        cut.release();
    }
    
}
