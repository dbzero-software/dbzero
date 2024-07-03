#include <gtest/gtest.h>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/PrefixCache.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <dbzero/core/memory/ResourceLock.hpp>
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
    
    TEST_F( PrefixCacheTest , testPageMapCanFindBestStateMatch )
    {
        db0::Storage0 dev_null;
        PageMap<ResourceLock> cut(dev_null.getPageSize());
        auto lock_1 = std::make_shared<ResourceLock>(dev_null, 0, 1, FlagSet<AccessOptions> {}, 0, 0);
        auto lock_2 = std::make_shared<ResourceLock>(dev_null, 0, 1, FlagSet<AccessOptions> {}, 0, 0);
        // state = 1, pages = 0 ... 9
        cut.insertRange(1, lock_1, 0, 10);
        // same range, different state
        cut.insertRange(11, lock_2, 0, 10);
        std::uint64_t state_num;
        ASSERT_EQ(cut.findRange(1, 0, 2, state_num), lock_1);
        ASSERT_EQ(cut.findRange(1, 0, 10, state_num), lock_1);
        ASSERT_EQ(cut.findRange(7, 5, 7, state_num), lock_1);
        ASSERT_EQ(cut.findRange(16, 4, 8, state_num), lock_2);
        ASSERT_EQ(cut.findRange(16, 0, 10, state_num), lock_2);

        // request invalid range
        ASSERT_ANY_THROW(cut.findRange(5, 8, 14, state_num));
    }

    TEST_F( PrefixCacheTest , testPrefixCacheCanTrackNegations )
    {        
        db0::Storage0 dev_null;
        PrefixCache cut(dev_null, nullptr);

        // state = 1, pages = 0 ... 9
        auto lock_1 = cut.createRange(0, 10, 0, 1, { AccessOptions::write });
        // same range, different state (11)
        auto lock_2 = cut.createRange(0, 10, 0, 11, { AccessOptions::write });
        
        std::uint64_t state_num;
        ASSERT_EQ(cut.findRange(0, 3, 1, {}, state_num), lock_1);
        ASSERT_EQ(cut.findRange(0, 10, 1, {}, state_num), lock_1);
        // state 14 is reported as existing
        ASSERT_NE(cut.findRange(4, 7, 14, {}, state_num), std::shared_ptr<ResourceLock> {});
        
        // add range as negated in state 12
        cut.markRangeAsMissing(4, 7, 12);
        // state not existing in cache
        ASSERT_EQ(cut.findRange(14, 17, 14, {}, state_num), std::shared_ptr<ResourceLock> {});
        cut.clear();
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
        ASSERT_EQ(cut.findRange(0, 1, 14, {}, state_num), lock_3);
        cut.clear();
    }
    
}
