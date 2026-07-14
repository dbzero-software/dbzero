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
    
    class LimitedMatrixTests: public MemspaceTestBase
    {
    public:
    };
    
    TEST_F( LimitedMatrixTests , testLimitedMatrixConstIteratorLoc )
    {
        using MatrixT = LimitedMatrix<std::uint64_t>;        
        MatrixT cut;
        cut.set({0,0}, 1);
        cut.set({5,0}, 3);
        cut.set({13,7}, 4);
        cut.set({11,2}, 5);
        cut.set({0,6}, 2);        
        
        std::vector<std::pair<std::uint32_t, std::uint32_t> > expected = {
            {0,0}, {0,6}, {5,0}, {11,2}, {13,7}
        };
        
        for (auto it = cut.begin(); it != cut.end(); ++it) {
            auto loc = it.loc();
            ASSERT_TRUE(cut.hasItem(loc));
            ASSERT_EQ(loc, expected.front());
            expected.erase(expected.begin());            
        }
    }

} 
