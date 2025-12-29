// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <thread>
#include <cstdlib>
#include <utility>

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
    
using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class VBVectorWorkspaceTests: public testing::Test
    {
    public:
        static constexpr const char *prefix_name = "my-test-prefix_1";
        static constexpr const char *file_name = "my-test-prefix_1.db0";
        
        void SetUp() override {
            drop(file_name);
        }
        
        void TearDown() override
        {            
            m_workspace.close();
            drop(file_name);
        }

    protected:
        Workspace m_workspace;        
    };

    TEST_F( VBVectorWorkspaceTests, testVBVectorInstanceCanBeAppendedBetweenTransactions )
    {
        auto fixture = m_workspace.getFixture(prefix_name);
        // create object and keep instance across multiple transactions
        v_bvector<int> cut(*fixture);
        cut.push_back(0);
        fixture->commit();
        cut.push_back(1);
        fixture->commit();
        ASSERT_EQ(cut.size(), 2);
    }
    
}