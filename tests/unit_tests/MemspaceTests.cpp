#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/memory/Memspace.hpp>

using namespace std;

namespace tests

{

    class MemspaceTests: public MemspaceTestBase
    {
    };
    
    TEST_F( MemspaceTests , testMemspaceAllocatorCanAlloc )
    {        
        // this test has been create to diagnose the crash on basic alloc
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        ASSERT_NO_THROW(memspace.getAllocator().alloc(100));
    }

}
