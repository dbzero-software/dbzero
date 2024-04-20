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
        db0::StorageView dev_null_view(dev_null, db0::Storage0::STATE_NULL);
        PageMap<ResourceLock> cut;
        auto lock_1 = std::make_shared<ResourceLock>(dev_null_view, 0, 1, FlagSet<AccessOptions> {});
        auto lock_2 = std::make_shared<ResourceLock>(dev_null_view, 0, 1, FlagSet<AccessOptions> {});
        // state = 1, pages = 0 ... 9
        cut.insertRange(1, lock_1, 0, 10);
        // same range, different state
        cut.insertRange(11, lock_2, 0, 10);
        ASSERT_EQ(cut.findRange(1, 0, 2), lock_1);
        ASSERT_EQ(cut.findRange(1, 0, 10), lock_1);
        ASSERT_EQ(cut.findRange(7, 5, 7), lock_1);
        ASSERT_EQ(cut.findRange(16, 4, 8), lock_2);
        ASSERT_EQ(cut.findRange(16, 0, 10), lock_2);

        // request invalid range
        ASSERT_ANY_THROW(cut.findRange(5, 8, 14));
    }
    
    TEST_F( PrefixCacheTest , testPrefixCacheCanTrackNegations )
    {        
        db0::Storage0 dev_null;
        db0::StorageView dev_null_view(dev_null, db0::Storage0::STATE_NULL);
        PrefixCache cut(dev_null_view, nullptr);
        auto page_size = dev_null.getPageSize();

        // state = 1, pages = 0 ... 9
        auto lock_1 = cut.createRange(0 * page_size, 1, 10 * page_size, { AccessOptions::write });
        // same range, different state (11)
        auto lock_2 = cut.createRange(0 * page_size, 11, 10 * page_size, { AccessOptions::write });
        
        ASSERT_EQ(cut.findRange(0 * page_size, 1, 3 * page_size), lock_1);
        ASSERT_EQ(cut.findRange(0 * page_size, 1, 10 * page_size), lock_1);
        // state 14 is reported as existing
        ASSERT_NE(cut.findRange(4 * page_size, 14, 3 * page_size), std::shared_ptr<ResourceLock> {});
        
        // add range as negated in state 12
        cut.markRangeAsMissing(4 * page_size, 12, 3 * page_size);
        // state not existing in cache
        ASSERT_EQ(cut.findRange(14 * page_size, 14, 3 * page_size), std::shared_ptr<ResourceLock> {});
    }
    
    TEST_F( PrefixCacheTest , testPrefixCacheNegationsClearedByCreateRange )
    {        
        db0::Storage0 dev_null;
        db0::StorageView dev_null_view(dev_null, db0::Storage0::STATE_NULL);
        PrefixCache cut(dev_null_view, nullptr);
        auto page_size = dev_null.getPageSize();

        // address, state_num, size
        cut.createRange(0, 1, page_size, { AccessOptions::write });
        cut.createRange(0, 11, page_size, { AccessOptions::write });
        // this simulates transaction 12 executed by an external process
        cut.markRangeAsMissing(0, 12, page_size);
        // this simulates retrieval of data from state_num = 12
        auto lock_3 = cut.createRange(0, 12, page_size, { AccessOptions::write });
        
        ASSERT_EQ(cut.findRange(0, 14, page_size), lock_3);
    }

}
