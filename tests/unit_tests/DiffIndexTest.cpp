#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/storage/DiffIndex.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <utils/utils.hpp>
#include <utils/diff_data_1.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class DiffIndexTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-prefix_1.db0";
        DiffIndexTest() = default;

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override {        
            drop(file_name);
        }
    };
    
    TEST_F( DiffIndexTest , testDiffIndexCanBeInstantiated )
    {
        DiffIndex cut(16 * 1024);
        ASSERT_EQ(cut.size(), 0);
    }

    TEST_F( DiffIndexTest , testDiffIndexInsertNewItems )
    {
        DiffIndex cut(16 * 1024);
        cut.insert(1, 1, 1);
        cut.insert(2, 1, 3);
        cut.insert(3, 1, 8);
        ASSERT_EQ(cut.size(), 3);
    }
    
    TEST_F( DiffIndexTest , testDiffIndexExpandExistingItems )
    {
        DiffIndex cut(16 * 1024);
        cut.insert(1, 1, 1);
        cut.insert(2, 1, 3);
        cut.insert(1, 3, 8);
        cut.insert(2, 3, 128);
        // 2 items of length = 2 should be inserted
        ASSERT_EQ(cut.size(), 2);
    }
    
    TEST_F( DiffIndexTest , testDiffIndexFindLower )
    {
        DiffIndex cut(16 * 1024);
        cut.insert(1, 1, 1);
        cut.insert(2, 1, 3);
        cut.insert(1, 3, 8);
        cut.insert(2, 3, 128);
        ASSERT_EQ(cut.findLower(1, 1), 1);
        ASSERT_EQ(cut.findLower(1, 2), 1);
        ASSERT_EQ(cut.findLower(1, 3), 3);
        ASSERT_EQ(cut.findLower(1, 16), 3);
        ASSERT_EQ(cut.findLower(2, 1), 1);
        ASSERT_EQ(cut.findLower(2, 11), 3);
        // not found
        ASSERT_EQ(cut.findLower(3, 11), 0);
    }
    
    TEST_F( DiffIndexTest , testDiffIndexFindUpper )
    {
        DiffIndex cut(16 * 1024);
        cut.insert(1, 2, 3);
        cut.insert(1, 4, 4);
        cut.insert(1, 5, 11);
        cut.insert(1, 9, 40);
        cut.insert(1, 12, 41);

        ASSERT_TRUE(cut.findUpper(1, 9));
        ASSERT_TRUE(cut.findUpper(1, 10));
        ASSERT_TRUE(cut.findUpper(1, 12));
        ASSERT_FALSE(cut.findUpper(1, 13));
    }

    TEST_F( DiffIndexTest , testDiffIndexFindUpperIssue1 )
    {        
        DiffIndex diff_index(16 * 1024);
        for (auto [page, state, storage]: getDiffIndexData1()) {
            diff_index.insert(page, state, storage);
        }

        auto item = diff_index.findUpper(4, 501);
        ASSERT_EQ(item.m_page_num, 4);
    }

}
