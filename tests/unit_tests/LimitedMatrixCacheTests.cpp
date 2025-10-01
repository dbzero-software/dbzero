#include <thread>
#include <cstdlib>
#include <utility>

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
#include <dbzero/core/collections/vector/VLimitedMatrix.hpp>
#include <dbzero/core/collections/vector/LimitedMatrixCache.hpp>
    
using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class LimitedMatrixCacheTests: public MemspaceTestBase
    {
    public:
    };

    TEST_F( LimitedMatrixCacheTests , testLMCacheDim1 )
    {
        using MatrixT = VLimitedMatrix<std::uint64_t, 32>;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        MatrixT mx(memspace);
        mx.set({0, 0}, 1);
        mx.set({13, 0}, 3);
        mx.set({11, 0}, 4);

        LimitedMatrixCache<MatrixT, std::uint64_t> cut(mx);
        ASSERT_EQ(cut.size(), 3);
    }

    TEST_F( LimitedMatrixCacheTests , testLMCacheDim2 )
    {
        using MatrixT = VLimitedMatrix<std::uint64_t, 32>;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        MatrixT mx(memspace);
        mx.set({0, 1}, 1);
        mx.set({13, 2}, 3);
        mx.set({11, 3}, 4);

        LimitedMatrixCache<MatrixT, std::uint64_t> cut(mx);
        ASSERT_EQ(cut.size(), 3);
    }
    
    TEST_F( LimitedMatrixCacheTests , testLMCachePopulatedWhenCreated )
    {
        using MatrixT = VLimitedMatrix<std::uint64_t, 32>;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        MatrixT mx(memspace);
        mx.set({0,0}, 1);
        mx.set({5,0}, 3);
        mx.set({13,7}, 4);
        mx.set({11,2}, 5);
        mx.set({0,6}, 2);
        
        LimitedMatrixCache<MatrixT, std::uint64_t> cut(mx);
        ASSERT_EQ(cut.size(), 5);
    }
    
} 
