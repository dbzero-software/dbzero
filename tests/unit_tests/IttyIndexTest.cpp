#include <gtest/gtest.h>
#include <dbzero/core/collections/b_index/IttyIndex.hpp>
#include <set>
#include <utils/TestBase.hpp>

namespace tests 

{

    class IttyIndexTest: public MemspaceTestBase
    {
    public:
    };
    
    TEST_F( IttyIndexTest , testIttyIndexActsAsVSpaceDataStructure ) 
    {
        auto memspace = getMemspace();

        // 1. create new itty_index / store 1 value
        std::uint64_t ptr;
        {
            db0::IttyIndex<std::uint64_t, std::uint64_t> index(memspace, 12345);
            ptr = index.getAddress();
        }
        // 2. open existing
        {
            db0::IttyIndex<std::uint64_t, std::uint64_t> index(std::make_pair(&memspace, ptr));
            // make sure this has proper value inside
            ASSERT_EQ(12345, index.getValue());
        }
    }

} 
