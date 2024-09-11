#include <cstdlib>
#include <utility>

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <dbzero/core/collections/vector/LimitedVector.hpp>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
    
using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class LimitedVectorTests: public MemspaceTestBase
    {
    };

    TEST_F( LimitedVectorTests , testLimitedVectorMakeFullPageAlloc )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");        
        LimitedVector<std::uint16_t> cut(memspace, memspace.getPageSize());

        ASSERT_TRUE(cut.getAddress() != 0);
        ASSERT_EQ(memspace.getAllocator().getAllocSize(cut.getAddress()), memspace.getPageSize());
    }

    TEST_F( LimitedVectorTests , testLimitedVectorThrowsOnInvalidBlockAccessed )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");        
        LimitedVector<std::uint16_t> cut(memspace, memspace.getPageSize());
        ASSERT_ANY_THROW(cut.get(0));
    }

    TEST_F( LimitedVectorTests , testLimitedVectorSetGetRandomValues )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");        
        LimitedVector<std::uint16_t> cut(memspace, memspace.getPageSize());
        std::vector<std::uint32_t> index { 11432, 33, 312, 14823, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        for (auto pos: index) {
            cut.set(pos, pos);
        }
        for (auto pos: index) {
            ASSERT_EQ(cut.get(pos), pos);
        }
    }

    TEST_F( LimitedVectorTests , testLimitedVectorCanAtomicIncrement )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");        
        LimitedVector<std::uint16_t> cut(memspace, memspace.getPageSize());
        std::vector<std::uint32_t> index {    0, 0, 1, 2, 2, 4, 2, 3, 4, 5, 6, 7, 8, 9, 11, 4, 0, 0, 11 };
        std::vector<std::uint32_t> expected { 1, 2, 1, 1, 2, 1, 3, 1, 2, 1, 1, 1, 1, 1, 1,  3, 3, 4, 2 };
        for (std::size_t i = 0; i < index.size(); ++i) {
            std::uint16_t value;
            cut.atomicInc(index[i], value);
            ASSERT_EQ(value, expected[i]);
        }
    }

}
