#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/storage/SparseIndexQuery.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class SparseIndexQueryTest: public testing::Test
    {    
    };

    TEST_F( SparseIndexQueryTest , testSparseIndexQueryNoDiffs )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_cut(16 * 1024);
        sparse_index.emplace(1, 1, 1);
        sparse_index.emplace(1, 3, 17);
        sparse_index.emplace(4, 7, 2343);
        
        // existing item
        {
            SparseIndexQuery cut(sparse_index, diff_cut, 1, 5);
            ASSERT_EQ(cut.first(), 17);
        }

        // non-existing item
        {
            SparseIndexQuery cut(sparse_index, diff_cut, 2, 5);
            ASSERT_EQ(cut.first(), 0);
        }
    }

    TEST_F( SparseIndexQueryTest , testSparseIndexQuerySingleDiff )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_cut(16 * 1024);
        sparse_index.emplace(1, 1, 1);
        // append diff-mutation for page 1
        diff_cut.insert(1, 2, 3);
        sparse_index.emplace(1, 3, 17);
        sparse_index.emplace(4, 7, 2343);
        
        // diff-mutated DP
        {
            SparseIndexQuery cut(sparse_index, diff_cut, 1, 2);
            ASSERT_EQ(cut.first(), 1);
            std::uint32_t state_num;
            std::uint64_t storage_page_num;
            ASSERT_TRUE(cut.next(state_num, storage_page_num));
            ASSERT_EQ(storage_page_num, 3);
        }
        
        // full DP
        {
            SparseIndexQuery cut(sparse_index, diff_cut, 1, 3);
            ASSERT_EQ(cut.first(), 17);
        }
    }
    
    TEST_F( SparseIndexQueryTest , testSparseIndexQueryMultipleDiffs )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_cut(16 * 1024);
        sparse_index.emplace(1, 1, 1);
        // append multiple diff-mutations for page 1
        diff_cut.insert(1, 2, 3);
        diff_cut.insert(1, 4, 4);
        diff_cut.insert(1, 5, 11);
        sparse_index.emplace(1, 8, 17);
        sparse_index.emplace(4, 7, 2343);
        diff_cut.insert(1, 9, 40);
        diff_cut.insert(1, 12, 41);
        
        // multi-diff-mutated DP
        SparseIndexQuery cut(sparse_index, diff_cut, 1, 7);
        ASSERT_EQ(cut.first(), 1);
        
        std::uint32_t state_num;
        std::uint64_t storage_page_num;
        std::vector<std::uint64_t> expected_page_num { 3, 4, 11 };
        for (auto expected : expected_page_num) {
            ASSERT_TRUE(cut.next(state_num, storage_page_num));
            ASSERT_EQ(storage_page_num, expected);
        }
        ASSERT_FALSE(cut.next(state_num, storage_page_num));
    }

}