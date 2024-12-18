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
            assert(item == items[index]);
            ++index;
        }
    }

}
