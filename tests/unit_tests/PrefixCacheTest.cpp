#include <gtest/gtest.h>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/ResourceLock.hpp>
#include <dbzero/core/memory/CacheRecycler.hpp>
#include <dbzero/core/storage/Storage0.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    class PrefixCacheTest: public testing::Test 
    {
    public:
        virtual void SetUp() override {            
        }

        virtual void TearDown() override {            
        }    
    };
    
    TEST_F( PrefixCacheTest , testPrefixCacheCanTrackNegations )
    {        
        db0::Storage0 dev_null;
        PrefixCache cut(dev_null, nullptr);

        // state = 1, pages = 0 ... 9
        auto lock_1 = cut.createRange(0, 10, 0, 1, { AccessOptions::write });
        // same range, different state (11)
        auto lock_2 = cut.createRange(0, 10, 0, 11, { AccessOptions::write });
        
        std::uint64_t state_num;
        int conflicts = 0;
        ASSERT_EQ(cut.findRange(0, 3, 1, {}, state_num, conflicts), lock_1);
        ASSERT_EQ(cut.findRange(0, 10, 1, {}, state_num, conflicts), lock_1);
        // state 14 is reported as existing
        ASSERT_NE(cut.findRange(4, 7, 14, {}, state_num, conflicts), std::shared_ptr<ResourceLock> {});
        
        // add range as negated in state 12
        cut.markRangeAsMissing(4, 7, 12);
        // state not existing in cache
        ASSERT_EQ(cut.findRange(14, 17, 14, {}, state_num, conflicts), std::shared_ptr<ResourceLock> {});
        ASSERT_FALSE(conflicts);
        cut.release();
    }
    
    TEST_F( PrefixCacheTest , testPrefixCacheNegationsClearedByCreateRange )
    {
        db0::Storage0 dev_null;
        PrefixCache cut(dev_null, nullptr);

        // address, state_num, size
        auto lock_1 = cut.createRange(0, 1, 0, 1, { AccessOptions::write });
        auto lock_2 = cut.createRange(0, 1, 0, 11, { AccessOptions::write });
        // this simulates transaction 12 executed by an external process
        cut.markRangeAsMissing(0, 1, 12);
        // this simulates retrieval of data from state_num = 12
        auto lock_3 = cut.createRange(0, 1, 0, 12, { AccessOptions::write });
        
        std::uint64_t state_num;
        int conflicts = 0;
        ASSERT_EQ(cut.findRange(0, 1, 14, {}, state_num, conflicts), lock_3);
        ASSERT_FALSE(conflicts);
        cut.release();
    }
    
    TEST_F( PrefixCacheTest , testPrefixCacheUpdateStateNumToAvoidCoW )
    {
        db0::Storage0 dev_null;
        db0::CacheRecycler cache_recycler(1 << 20u);
        PrefixCache cut(dev_null, &cache_recycler);

        // first page, end page, read state num, state num
        // create page in state #1
        {
            auto lock = cut.createRange(0, 1, 0, 1, { AccessOptions::write });
            lock->flush();
        }    
        // request state #2 for read-write (note that lock has been released)
        std::uint64_t read_state_num;
        int conflicts = 0;
        auto lock = cut.findRange(0, 1, 2, { AccessOptions::read, AccessOptions::write }, read_state_num, conflicts);
        ASSERT_TRUE(lock);

        // make sure only 1 lock is cached (no CoW), since upgrade took place
        ASSERT_EQ(cache_recycler.size(), lock->size());
        
        // now, try reading the state #1 version of the page
        auto lock_1 = cut.findRange(0, 1, 1, { AccessOptions::read }, read_state_num, conflicts);
        // lock of this version should be no longer present in cache
        ASSERT_FALSE(lock_1);
        ASSERT_FALSE(conflicts);

        cut.release();
    }
    
}
