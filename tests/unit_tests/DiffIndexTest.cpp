#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/storage/DiffIndex.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <utils/utils.hpp>

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

}
