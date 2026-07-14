// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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

    template <typename T> struct StringAdapter
    {
        std::string operator()(std::pair<std::uint32_t, std::uint32_t>, const T &item) const {
            return std::to_string(item);
        }
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

    TEST_F( LimitedMatrixCacheTests , testLMGetExistingItems )
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
        ASSERT_EQ(*cut.tryGet({0,0}), 1);
        ASSERT_EQ(*cut.tryGet({5,0}), 3);
        ASSERT_EQ(*cut.tryGet({13,7}), 4);
        ASSERT_EQ(*cut.tryGet({11,2}), 5);
        ASSERT_EQ(*cut.tryGet({0,6}), 2);        
    }
    
    TEST_F( LimitedMatrixCacheTests , testLMTryGetNonExistingItems )
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
        ASSERT_EQ(cut.tryGet({1,0}), nullptr);
        ASSERT_EQ(cut.tryGet({0,1}), nullptr);
        ASSERT_EQ(cut.tryGet({5,1}), nullptr);
        ASSERT_EQ(cut.tryGet({13,0}), nullptr);
        ASSERT_EQ(cut.tryGet({11,3}), nullptr);
        ASSERT_EQ(cut.tryGet({0,5}), nullptr);    
    }
    
    TEST_F( LimitedMatrixCacheTests , testLMCacheIterator )
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
        std::vector<std::uint64_t> expected = {1, 2, 3, 5, 4};
        
        auto it = cut.cbegin(), end = cut.cend();
        std::size_t idx = 0;
        for ( ; it != end; ++it, ++idx) {
            ASSERT_LT(idx, expected.size());            
            ASSERT_EQ(*it, expected[idx]);
        }
        ASSERT_EQ(idx, expected.size());
    }
    
    TEST_F( LimitedMatrixCacheTests , testLMCacheRefresh )
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
        mx.set({7,1}, 6);
        ASSERT_TRUE(cut.refresh());
        ASSERT_EQ(cut.size(), 6);
        ASSERT_FALSE(cut.refresh());
        ASSERT_EQ(cut.size(), 6);

        mx.set({18,0}, 13);
        ASSERT_TRUE(cut.refresh());
        ASSERT_EQ(cut.size(), 7);
    }
    
    TEST_F( LimitedMatrixCacheTests , testLMCacheReload )
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
        mx.set({13, 7}, 555);
        cut.reload({13, 7});
        ASSERT_EQ(*cut.tryGet({13, 7}), 555);
    }

    TEST_F( LimitedMatrixCacheTests , testLMCacheWithCustomAdapter )
    {
        using MatrixT = VLimitedMatrix<std::uint64_t, 32>;
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        MatrixT mx(memspace);
        mx.set({0,0}, 1);
        mx.set({5,0}, 3);
        mx.set({13,7}, 4);
        mx.set({11,2}, 5);
        mx.set({0,6}, 2);
        
        // NOTE: in cache, items are adapted to strings
        LimitedMatrixCache<MatrixT, std::string, StringAdapter<std::uint64_t> > cut(mx);
        ASSERT_EQ(*cut.tryGet({0,0}), "1");
        ASSERT_EQ(*cut.tryGet({5,0}), "3");
        ASSERT_EQ(*cut.tryGet({13,7}), "4");
    }

    TEST_F( LimitedMatrixCacheTests , testLMCacheAutoRefreshOnTryGet )
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
        ASSERT_FALSE(cut.tryGet({7,1}));
        mx.set({7,1}, 6);
        // NOTE: element should be auto-refreshed on tryGet
        ASSERT_TRUE(cut.tryGet({7,1}));
    }

} 
