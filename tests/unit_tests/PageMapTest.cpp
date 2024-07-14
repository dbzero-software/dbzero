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
        PageMap<ResourceLock> cut(dev_null.getPageSize());
        std::uint64_t state_num;
        ASSERT_EQ(cut.findRange(1, 0, 2, state_num), nullptr);
        ASSERT_EQ(cut.findRange(1, 0, 10, state_num), nullptr);
        ASSERT_EQ(cut.findRange(7, 5, 7, state_num), nullptr);
        ASSERT_EQ(cut.findRange(16, 4, 8, state_num), nullptr);
        ASSERT_EQ(cut.findRange(16, 0, 10, state_num), nullptr);
    }
    
    TEST_F( PageMapTest , testPageMapCanFindBestStateMatch )
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

}
