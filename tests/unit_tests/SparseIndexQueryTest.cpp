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
        
        // sparse index / diff index interleaved items
        {
            SparseIndexQuery cut(sparse_index, diff_cut, 1, 11);
            ASSERT_EQ(cut.first(), 17);

            std::uint32_t state_num;
            std::uint64_t storage_page_num;
            std::vector<std::uint64_t> expected_page_num { 40 };
            for (auto expected : expected_page_num) {
                ASSERT_TRUE(cut.next(state_num, storage_page_num));
                ASSERT_EQ(storage_page_num, expected);
            }
            ASSERT_FALSE(cut.next(state_num, storage_page_num));
        }
        
        // multi-diff-mutated DP
        {
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

    TEST_F( SparseIndexQueryTest , testSparseIndexQueryWithLongDiffsChain )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);
        sparse_index.emplace(1, 1, 1);
        sparse_index.emplace(4, 7, 2343);
        // append a long chain of diffs
        for (std::uint32_t i = 3; i < 100; ++i) {
            diff_index.insert(1, i, i + 1);
            diff_index.insert(2, i, i + 1);
            diff_index.insert(3, i, i + 1);
        }
        ASSERT_TRUE(diff_index.size() > 3);
        
        // multi-diff-mutated DP
        SparseIndexQuery cut(sparse_index, diff_index, 1, 45);
        ASSERT_EQ(cut.first(), 1);
        
        std::uint32_t state_num, last_state_num;
        std::uint64_t storage_page_num, last_storage_page_num;
        std::uint64_t expected_page_num = 4;
        while (cut.next(state_num, storage_page_num)) {
            ASSERT_EQ(storage_page_num, expected_page_num);
            ++expected_page_num;
            last_state_num = state_num;
            last_storage_page_num = storage_page_num;
        }
        
        ASSERT_EQ(last_state_num, 45);
        ASSERT_EQ(last_storage_page_num, 46);
    }
    
    TEST_F( SparseIndexQueryTest , testFindMutationQuery )
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

        // test positive cases
        // query params: page_num, state_num, expected state num
        std::vector<std::tuple<std::uint64_t, std::uint32_t, std::uint32_t>> query_params {
            { 1, 11, 9 },
            { 1, 7, 5 },
            { 1, 8, 8 },
            { 1, 9, 9 },
            { 1, 12, 12 },
            { 1, 13, 12 },
            { 1, 16, 12 },
            { 4, 7, 7 },
            { 4, 13, 7 }
        };

        for (auto [page_num, state_num, expected_state_num] : query_params) {
            std::uint64_t mutation_id;
            ASSERT_TRUE(tryFindMutation(sparse_index, diff_cut, page_num, state_num, mutation_id));
            ASSERT_EQ(mutation_id, expected_state_num);
        }

        // test negative cases
        // query params: page_num, state_num
        std::vector<std::tuple<std::uint64_t, std::uint32_t>> negative_query_params {
            { 2, 11 },
            { 3, 1 },
            { 4, 1 },
            { 4, 6 },
        };

        for (auto [page_num, state_num] : negative_query_params) {
            std::uint64_t mutation_id;
            ASSERT_FALSE(tryFindMutation(sparse_index, diff_cut, page_num, state_num, mutation_id));
        }
    }
    
}