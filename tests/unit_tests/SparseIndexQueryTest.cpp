// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/memory/config.hpp>
#include <dbzero/core/storage/SparseIndexQuery.hpp>
#include <utils/diff_data_1.hpp>

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
        // page num, state num, storage page num
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
            { 1, 11, 9 }, { 1, 7, 5 }, { 1, 8, 8 }, { 1, 9, 9 }, { 1, 12, 12 },
            { 1, 13, 12 }, { 1, 16, 12 }, { 4, 7, 7 }, { 4, 13, 7 }
        };

        for (auto [page_num, state_num, expected_state_num] : query_params) {
            StateNumType mutation_id;
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
            StateNumType mutation_id;
            ASSERT_FALSE(tryFindMutation(sparse_index, diff_cut, page_num, state_num, mutation_id));
        }
    }
    
    TEST_F( SparseIndexQueryTest , testSparseIndexQueryIssue1 )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);
        sparse_index.emplace(4, 500, 100);
        for (auto [page, state, storage]: getDiffIndexData1()) {
            diff_index.insert(page, state, storage);
        }

        SparseIndexQuery cut(sparse_index, diff_index, 4, 1055);
        ASSERT_EQ(cut.first(), 100);
        StateNumType state_num;
        std::uint64_t storage_page_num;
        ASSERT_TRUE(cut.next(state_num, storage_page_num));
        ASSERT_EQ(storage_page_num, 5348);
        ASSERT_FALSE(cut.next(state_num, storage_page_num));
    }
    
    TEST_F( SparseIndexQueryTest , testSparseIndexQueryLeftLessThan )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);
        sparse_index.emplace(4, 500, 100);
        sparse_index.emplace(3, 500, 300);
        for (auto [page, state, storage]: getDiffIndexData1()) {
            diff_index.insert(page, state, storage);
        }
        
        {
            SparseIndexQuery cut(sparse_index, diff_index, 4, 1055);
            cut.first();
            ASSERT_TRUE(cut.leftLessThan(2));
            ASSERT_FALSE(cut.leftLessThan(1));
        }
        
        {
            SparseIndexQuery cut(sparse_index, diff_index, 3, 900);            
            cut.first();
            ASSERT_FALSE(cut.leftLessThan(6));
        }
    }
    
    TEST_F( SparseIndexQueryTest , testSparseIndexQueryLessThan )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);
        sparse_index.emplace(4, 500, 100);
        sparse_index.emplace(3, 500, 300);
        for (auto [page, state, storage]: getDiffIndexData1()) {
            diff_index.insert(page, state, storage);
        }
        
        StateNumType state_num;
        std::uint64_t storage_page_num;
        {
            SparseIndexQuery cut(sparse_index, diff_index, 4, 1055);
            cut.first();
            ASSERT_TRUE(cut.lessThan(3));
            ASSERT_FALSE(cut.lessThan(2));
            // result not affected by first / next calls
            cut.first();
            cut.next(state_num, storage_page_num);
            ASSERT_TRUE(cut.lessThan(3));
            ASSERT_FALSE(cut.lessThan(2));
        }
        
        {
            SparseIndexQuery cut(sparse_index, diff_index, 3, 900);            
            cut.first();
            ASSERT_FALSE(cut.lessThan(7));
        }
    }
    
    TEST_F( SparseIndexQueryTest , testSparseIndexQueryStartingFromDiffPage )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);
        // append multiple diff-mutations for page 1 without base page (i.e. 0x0 based)
        diff_index.insert(1, 2, 3);
        diff_index.insert(1, 4, 4);
        sparse_index.emplace(1, 7, 1);
        diff_index.insert(1, 8, 11);
        diff_index.insert(1, 9, 12);
        sparse_index.emplace(4, 7, 17);
        sparse_index.emplace(4, 8, 2343);
        diff_index.insert(4, 9, 40);

        SparseIndexQuery cut(sparse_index, diff_index, 1, 5);
        ASSERT_EQ(cut.first(), 0);
        StateNumType state_num;
        std::uint64_t storage_page_num;
        ASSERT_TRUE(cut.next(state_num, storage_page_num));
        ASSERT_EQ(storage_page_num, 3);
        ASSERT_TRUE(cut.next(state_num, storage_page_num));
        ASSERT_EQ(storage_page_num, 4);
        ASSERT_FALSE(cut.next(state_num, storage_page_num));
    }

    TEST_F( SparseIndexQueryTest , testSparseIndexQueryEmpty )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);        
        std::vector<std::tuple<std::uint64_t, std::uint32_t, std::uint32_t>> diff_data {
            { 1, 2, 3 }, { 1, 4, 4 }, { 1, 8, 11 }, { 1, 9, 12 },
            { 5, 2, 22 }, { 5, 3, 23 }, { 5, 4, 24 }, { 5, 5, 25 }, { 5, 6, 26 },
            { 5, 7, 27 }, { 5, 8, 28 }, { 5, 9, 29 }, { 5, 10, 30 }, { 5, 11, 31 },
            { 5, 12, 32 }, { 5, 13, 33 }, { 5, 14, 34 }, { 5, 15, 35 }, { 5, 16, 36 }
        };
        for (auto [page, state, storage]: diff_data) {
            diff_index.insert(page, state, storage);
        }
        
        sparse_index.emplace(1, 7, 1);
        sparse_index.emplace(4, 7, 17);
        sparse_index.emplace(4, 8, 2343);
        
        // page / state / is empty
        auto query_data = std::vector<std::tuple<int, int, bool> > {
            { 4, 7, false }, { 1, 1, true }, { 1, 2, false }, { 1, 3, false },
            { 1, 7, false }, { 4, 4, true }, { 4, 6, true }, { 5, 1, true }, 
            { 5, 2, false }, { 5, 3, false }, { 5, 4, false }, 
            { 5, 12, false }, { 5, 3, false }, { 5, 24, false }, 
            { 15, 3, true }
        };
        
        for (auto [page, state, is_empty]: query_data) {
            SparseIndexQuery cut(sparse_index, diff_index, page, state);
            ASSERT_EQ(cut.empty(), is_empty);
        }
    }

    TEST_F( SparseIndexQueryTest , testSparseIndexQueryZeroBasedChain )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);
        std::vector<std::tuple<std::uint64_t, std::uint32_t, std::uint32_t>> diff_data {
            { 1, 2, 2 }, { 1, 3, 3 }, { 1, 4, 4 }, { 1, 5, 5 }, { 1, 6, 6 }, { 1, 7, 7 },
            { 1, 8, 8 }, { 1, 9, 9 }, { 1, 10, 10 }, { 1, 11, 11 }, { 1, 12, 12 }, 
            { 1, 13, 13 }, { 1, 14, 14 }, { 1, 15, 15 }, { 1, 16, 16 }, { 1, 17, 17 }, 
            { 1, 18, 18 }, { 1, 19, 19 }, { 1, 20, 20 }, { 1, 21, 21 }, { 1, 22, 22 },
            { 1, 24, 24 }
        };
        
        for (auto [page, state, storage]: diff_data) {
            diff_index.insert(page, state, storage);
        }
        // make sure at least 2 items have been crated
        ASSERT_TRUE(diff_index.size() > 1);
        sparse_index.emplace(1, 23, 23);

        SparseIndexQuery cut(sparse_index, diff_index, 1, 24);
        ASSERT_EQ(cut.empty(), false);
        ASSERT_EQ(cut.first(), 23);
        StateNumType state_num;
        std::uint64_t storage_page_num;        
        ASSERT_TRUE(cut.next(state_num, storage_page_num));
        ASSERT_EQ(storage_page_num, 24);
        ASSERT_FALSE(cut.next(state_num, storage_page_num));
    }
    
    TEST_F( SparseIndexQueryTest , testSparseIndexQueryZeroBasedDiffChain )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);
        std::vector<std::tuple<std::uint64_t, std::uint32_t, std::uint32_t>> diff_data {
            { 1, 2, 2 }, { 1, 3, 3 }, { 1, 4, 4 }, { 1, 5, 5 }, { 1, 6, 6 }, { 1, 7, 7 },
            { 1, 8, 8 }, { 1, 9, 9 }, { 1, 10, 10 }, { 1, 11, 11 }, { 1, 12, 12 }, 
            { 1, 13, 13 }, { 1, 14, 14 }, { 1, 15, 15 }, { 1, 16, 16 }, { 1, 17, 17 }, 
            { 1, 18, 18 }, { 1, 19, 19 }, { 1, 20, 20 }, { 1, 21, 21 }, { 1, 22, 22 },
            { 1, 24, 24 }
        };

        for (auto [page, state, storage]: diff_data) {
            diff_index.insert(page, state, storage);
        }
        // make sure at least 2 items have been crated
        ASSERT_TRUE(diff_index.size() > 1);

        SparseIndexQuery cut(sparse_index, diff_index, 1, 24);
        ASSERT_EQ(cut.empty(), false);
        ASSERT_EQ(cut.first(), 0);

        std::vector<std::uint64_t> expected_page_num { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
            13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 24 };

        StateNumType state_num;
        std::uint64_t storage_page_num;
        for (auto expected : expected_page_num) {
            ASSERT_TRUE(cut.next(state_num, storage_page_num));
            ASSERT_EQ(storage_page_num, expected);
        }
        ASSERT_FALSE(cut.next(state_num, storage_page_num));
    }

    TEST_F( SparseIndexQueryTest , testSparseIndexQuery_Issue1 )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);        
        diff_index.insert(1, 2, 2);
        diff_index.insert(1, 3, 3);
        sparse_index.emplace(1, 4, 4);
        diff_index.insert(1, 5, 5);
        diff_index.insert(1, 6, 6);
        
        SparseIndexQuery cut(sparse_index, diff_index, 1, 5);
        ASSERT_EQ(cut.first(), 4);
        StateNumType state_num;
        std::uint64_t storage_page_num;
        ASSERT_TRUE(cut.next(state_num, storage_page_num));
        ASSERT_EQ(storage_page_num, 5);
        ASSERT_FALSE(cut.next(state_num, storage_page_num));
    }
    
    TEST_F( SparseIndexQueryTest , testFindMutationOfZeroBasedDPs )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);
        std::vector<std::tuple<std::uint64_t, std::uint32_t, std::uint32_t>> diff_data {
            { 1, 2, 3 }, { 1, 4, 4 }, { 1, 8, 11 }, { 1, 9, 12 },
            { 5, 2, 22 }, { 5, 3, 23 }, { 5, 4, 24 }, { 5, 5, 25 }, { 5, 6, 26 },
            { 5, 7, 27 }, { 5, 8, 28 }, { 5, 9, 29 }, { 5, 10, 30 }, { 5, 11, 31 },
            { 5, 12, 32 }, { 5, 13, 33 }, { 5, 14, 34 }, { 5, 15, 35 }, { 5, 16, 36 }
        };
        for (auto [page, state, storage]: diff_data) {
            diff_index.insert(page, state, storage);
        }
        
        sparse_index.emplace(1, 7, 1);
        sparse_index.emplace(4, 7, 17);
        sparse_index.emplace(4, 8, 2343);
        
        // page / state / mutation ID
        auto query_data = std::vector<std::tuple<int, int, int> > {
            { 4, 7, 7 }, { 1, 1, 0 }, { 1, 2, 2 }, { 1, 3, 2 }, { 1, 4, 4 }, { 1, 5, 4 },
            { 1, 7, 7 }, { 1, 8, 8 }, { 1, 9, 9 }, { 1, 10, 9 }, { 1, 11, 9 }, 
            { 4, 4, 0 }, { 4, 6, 0 }, { 5, 1, 0 },
            { 5, 2, 2 }, { 5, 3, 3 }, { 5, 4, 4 }, 
            { 5, 12, 12 }, { 5, 13, 13 }, { 5, 24, 16 }, 
            { 15, 3, 0 }
        };

        for (auto [page, state, expected_mutation_id]: query_data) {
            StateNumType mutation_id = 0;
            db0::tryFindMutation(sparse_index, diff_index, page, state, mutation_id);
            ASSERT_EQ(mutation_id, expected_mutation_id);
        }
    }
    
    TEST_F( SparseIndexQueryTest , testSparseIndexStartingFromDiff )
    {
        SparseIndex sparse_index(16 * 1024);
        DiffIndex diff_index(16 * 1024);
        sparse_index.emplace(1, 1, 1);
        sparse_index.emplace(1, 3, 17);
        sparse_index.emplace(4, 7, 2343);

        diff_index.insert(1, 2, 3);
        diff_index.insert(7, 1, 1);

        // NOTE: history of page = 7 starts with the diff-write
        SparseIndexQuery cut(sparse_index, diff_index, 7, 1);
        ASSERT_FALSE(cut.empty());
    }

}