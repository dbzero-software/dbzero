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

        ASSERT_EQ(cut.size(), 100u);
    }
    
    TEST_F( VLimitedMatrixTests , testPushBackToDim1AndDim2 )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t> cut(memspace);
        for (std::uint32_t i = 0; i < 10; ++i) {
            cut.push_back(i * 10);
        }
        cut.push_back(999, 1);
        cut.push_back(1000, 18);

        ASSERT_EQ(cut.size(), 12u);
    }

    TEST_F( VLimitedMatrixTests , testGetExistingItems )
    {
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        VLimitedMatrix<std::uint64_t> cut(memspace);
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
        VLimitedMatrix<std::uint64_t> cut(memspace);
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
        VLimitedMatrix<std::uint64_t> cut(memspace);
        cut.set({0,0}, 123);
        cut.set({5,0}, 456);
        cut.set({10, 1}, 789);
        cut.set({11, 18}, 1001);

        ASSERT_EQ(cut.size(), 12u);
        ASSERT_EQ(cut.get({0,0}), 123u);
        ASSERT_EQ(cut.get({5,0}), 456u);
        ASSERT_EQ(cut.get({10, 1}), 789u);
        ASSERT_EQ(cut.get({11, 18}), 1001u);

        ASSERT_FALSE(cut.tryGet({0,3}));
        ASSERT_FALSE(cut.tryGet({6,0}));
        ASSERT_FALSE(cut.tryGet({10,2}));
    }

} 
