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
    };

    TEST_F( MUStoreTest , testMUStoreInstanceCreation )
    {        
        auto memspace = getMemspace();

        using MU_Store = v_object<db0::o_mu_store>;
        ASSERT_NO_THROW( MU_Store(memspace, 256) );
    }

}
