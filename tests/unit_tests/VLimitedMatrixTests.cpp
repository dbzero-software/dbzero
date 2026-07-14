// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <thread>
#include <cstdlib>
#include <utility>

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <dbzero/core/collections/vector/VLimitedMatrix.hpp>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>
#include <dbzero/workspace/Workspace.hpp>
    
using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class VLimitedMatrixTests: public MemspaceTestBase
    {
    };
    
    TEST_F( VLimitedMatrixTests , testEmptyLimitedMatrixCanBeCreated )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t> cut(memspace);
        ASSERT_TRUE(cut.getAddress().isValid());
    }

    TEST_F( VLimitedMatrixTests , testLimitedMatrixCanBeOpened )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        Address addr;
        {
            VLimitedMatrix<std::uint64_t> cut(memspace);
            addr = cut.getAddress();
        }
        VLimitedMatrix<std::uint64_t> cut(memspace.myPtr(addr));
        ASSERT_TRUE(cut.getAddress().isValid());
    }
    
    TEST_F( VLimitedMatrixTests , testPushBackToDim1 )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t> cut(memspace);
        for (std::uint32_t i = 0; i < 100; ++i) {
            cut.push_back(i * 10);
        }

        ASSERT_EQ(cut.size().first, 100u);
    }
    
    TEST_F( VLimitedMatrixTests , testPushBackToDim1AndDim2 )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t, 32> cut(memspace);
        for (std::uint32_t i = 0; i < 10; ++i) {
            cut.push_back(i * 10);
        }
        cut.push_back(999, 1);
        cut.push_back(1000, 18);

        ASSERT_EQ(cut.size().first, 12u);
    }

    TEST_F( VLimitedMatrixTests , testGetExistingItems )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t, 32> cut(memspace);
        for (std::uint32_t i = 0; i < 10; ++i) {
            cut.push_back(i * 10);
        }
        cut.push_back(999, 1);
        cut.push_back(1000, 18);
        
        ASSERT_EQ(cut.get({0,0}), 0u);   
        ASSERT_EQ(cut.get({5,0}), 50u);
        // get from dim2
        ASSERT_EQ(cut.get({10, 1}), 999);
        ASSERT_EQ(cut.get({11, 18}), 1000);
    }
    
    TEST_F( VLimitedMatrixTests , testTryGetNonExistingItems )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t, 32> cut(memspace);
        for (std::uint32_t i = 0; i < 10; ++i) {
            cut.push_back(i * 10);
        }
        cut.push_back(999, 1);
        cut.push_back(1000, 18);
        
        ASSERT_FALSE(cut.tryGet({0, 3}));
        ASSERT_FALSE(cut.tryGet({10, 2}));
        ASSERT_FALSE(cut.tryGet({18, 0}));
        ASSERT_FALSE(cut.tryGet({13, 1}));
    }

    TEST_F( VLimitedMatrixTests , testSetNewItems )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t, 32> cut(memspace);
        cut.set({0,0}, 123);
        cut.set({5,0}, 456);
        cut.set({10, 1}, 789);
        cut.set({11, 18}, 1001);
        
        ASSERT_EQ(cut.size().first, 12u);
        ASSERT_EQ(cut.get({0,0}), 123u);
        ASSERT_EQ(cut.get({5,0}), 456u);
        ASSERT_EQ(cut.get({10, 1}), 789u);
        ASSERT_EQ(cut.get({11, 18}), 1001u);

        ASSERT_FALSE(cut.tryGet({0,3}));
        ASSERT_FALSE(cut.tryGet({6,0}));
        ASSERT_FALSE(cut.tryGet({10,2}));
    }
    
    TEST_F( VLimitedMatrixTests , testFindUnassignedKey )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t, 32> cut(memspace);
        cut.set({0,0}, 123);    
        cut.set({10, 1}, 789);
        // 1 value already assigned @dim2
        unsigned int count = 1;
        for (;;) {
            auto key_2 = cut.findUnassignedKey(10);
            if (!key_2) {                
                break;
            }
            cut.set(std::make_pair(10, *key_2), 0);
            ++count;
        }
        // make sure all possible keys have been assigned @dim2
        ASSERT_EQ(count, cut.maxDim2());
    }
    
    TEST_F( VLimitedMatrixTests , testLimitedMatrixConstIteratorOverBothDims )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t, 32> cut(memspace);
        cut.set({0,0}, 1);
        cut.set({5,0}, 3);
        cut.set({13,7}, 4);
        cut.set({11,2}, 5);
        cut.set({0,6}, 2);

        std::vector<std::uint64_t> expected {1, 2, 3, 5, 4};
        auto it = cut.cbegin(), end = cut.cend();
        unsigned int count = 0;
        for (; it != end; ++it, ++count) {
            ASSERT_EQ(*it, expected[count]);
        }
        ASSERT_EQ(count, 5u);
    }

    TEST_F( VLimitedMatrixTests , testLimitedMatrixConstIteratorOverDim2 )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        // create with Dim1 empty
        VLimitedMatrix<std::uint64_t, 32> cut(memspace);
        cut.set({0,1}, 1);
        cut.set({0,5}, 2);
        cut.set({0,17}, 3);
        cut.set({3,3}, 4);
        cut.set({3,5}, 5);
        cut.set({7,1}, 6);

        std::vector<std::uint64_t> expected {1, 2, 3, 4, 5, 6};
        auto it = cut.cbegin(), end = cut.cend();        
        unsigned int count = 0;
        for (; it != end; ++it, ++count) {
            ASSERT_EQ(*it, expected[count]);
        }        
        ASSERT_EQ(count, 6u);
    }

    TEST_F( VLimitedMatrixTests , testLimitedMatrixIteratorLoc )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        // create with Dim1 empty
        VLimitedMatrix<std::uint64_t, 32> cut(memspace);
        cut.set({0,1}, 1);
        cut.set({0,5}, 2);
        cut.set({0,17}, 3);
        cut.set({3,3}, 4);
        cut.set({3,5}, 5);
        cut.set({7,1}, 6);

        std::vector<std::pair<std::uint32_t, std::uint32_t> > expected_loc {
            { 0, 1 }, { 0, 5 }, { 0, 17 }, { 3, 3 }, { 3, 5 }, { 7, 1 }
        };

        auto it = cut.cbegin(), end = cut.cend();
        unsigned int count = 0;
        for (; it != end; ++it, ++count) {
            ASSERT_EQ(it.loc(), expected_loc[count]);
        }
    }

} 
