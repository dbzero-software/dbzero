// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/storage/SparseIndex.hpp>
#include <dbzero/core/storage/DiffIndex.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <dbzero/core/storage/SparseIndexQuery.hpp>
#include <dbzero/core/memory/config.hpp>
#include <utils/utils.hpp>
#include <utils/diff_data_1.hpp>
#include <filesystem>

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

    class DiffIndexEraseTestAdapter: public DiffIndex
    {
    public:
        using DiffIndex::DiffIndex;

        bool eraseDescriptor(PageNumT page_num, StateNumT state_num) {
            return super_t::erase(page_num, state_num);
        }

        std::size_t eraseDescriptorsBelow(PageNumT page_num, StateNumT state_num) {
            return super_t::eraseBelow(page_num, state_num);
        }

        std::size_t eraseDescriptorRange(PageNumT page_num, std::optional<StateNumT> first_state_num = {},
            std::optional<StateNumT> last_state_num = {}) {
            return super_t::eraseRange(page_num, first_state_num, last_state_num);
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

    TEST_F( DiffIndexTest , testDiffIndexSparseIndexBaseCanEraseExactDescriptor )
    {
        DiffIndexEraseTestAdapter cut(512);
        cut.insert(1, 1, 10);
        cut.insert(2, 1, 20);
        cut.insert(3, 1, 30);

        ASSERT_TRUE(cut.eraseDescriptor(2, 1));
        ASSERT_FALSE(cut.eraseDescriptor(2, 1));
        ASSERT_EQ(cut.size(), 2u);
        ASSERT_EQ(cut.findLower(1, 1), 1u);
        ASSERT_EQ(cut.findLower(2, 1), 0u);
        ASSERT_EQ(cut.findLower(3, 1), 1u);
    }

    TEST_F( DiffIndexTest , testDiffIndexSparseIndexBaseEraseBelowDescriptorEdgeCasesWithSmallNodes )
    {
        DiffIndexEraseTestAdapter cut(512);
        constexpr std::uint64_t storage_step = 1ull << 32;
        for (std::uint32_t state_num = 1; state_num <= 40; ++state_num) {
            cut.insert(1, state_num, storage_step * state_num);
            cut.insert(2, state_num, storage_step * (100 + state_num));
        }
        ASSERT_GT(cut.size(), 2u);

        auto original_size = cut.size();
        ASSERT_EQ(cut.eraseDescriptorsBelow(1, 0), 0u);
        ASSERT_EQ(cut.eraseDescriptorsBelow(1, 1), 0u);
        ASSERT_EQ(cut.eraseDescriptorsBelow(99, 20), 0u);
        ASSERT_EQ(cut.size(), original_size);

        auto removed = cut.eraseDescriptorsBelow(1, std::numeric_limits<std::uint32_t>::max());
        ASSERT_GT(removed, 0u);
        ASSERT_EQ(cut.size(), original_size - removed);
        ASSERT_EQ(cut.findLower(1, 40), 0u);
        ASSERT_EQ(cut.findLower(2, 40), 40u);

        auto page_2_descriptor_count = cut.size();
        ASSERT_EQ(cut.eraseDescriptorsBelow(1, std::numeric_limits<std::uint32_t>::max()), 0u);
        ASSERT_EQ(cut.eraseDescriptorsBelow(2, std::numeric_limits<std::uint32_t>::max()), page_2_descriptor_count);
        ASSERT_TRUE(cut.empty());
    }

    TEST_F( DiffIndexTest , testDiffIndexSparseIndexBaseEraseRangeDescriptorOptionalBounds )
    {
        DiffIndexEraseTestAdapter cut(512);
        constexpr std::uint64_t storage_step = 1ull << 32;
        for (std::uint32_t state_num = 1; state_num <= 12; ++state_num) {
            cut.insert(1, state_num, storage_step * state_num);
            cut.insert(2, state_num, storage_step * (100 + state_num));
        }
        auto original_size = cut.size();
        ASSERT_GT(original_size, 2u);

        auto removed_middle = cut.eraseDescriptorRange(1, 4, 8);
        ASSERT_GT(removed_middle, 0u);
        ASSERT_EQ(cut.size(), original_size - removed_middle);
        ASSERT_EQ(cut.findLower(2, 12), 12u);

        auto removed_tail = cut.eraseDescriptorRange(1, 8, {});
        ASSERT_GT(removed_tail, 0u);
        ASSERT_EQ(cut.size(), original_size - removed_middle - removed_tail);
        ASSERT_EQ(cut.findLower(2, 12), 12u);

        auto removed_page_2 = cut.eraseDescriptorRange(2);
        ASSERT_GT(removed_page_2, 0u);
        ASSERT_EQ(cut.findLower(2, 12), 0u);
        ASSERT_EQ(cut.size(), original_size - removed_middle - removed_tail - removed_page_2);
    }

    TEST_F( DiffIndexTest , DISABLED_testDiffIndexInsertThenQuery )
    {   
        auto ops = loadArray("./tests/files/diff_index_ops.csv");
        SparseIndex sparse_index(512);
        DiffIndex diff_index(512);
        std::vector<std::pair<std::uint64_t, std::uint32_t>> queries;
        unsigned int count = 0;
        
        auto run_queries = [&]() -> bool {
            int step = queries.size() / 1000;
            for (unsigned int i = 0; i < queries.size(); i += step) {
                auto &query = queries[i];
                SparseIndexQuery cut(sparse_index, diff_index, query.first, query.second);
                // make sure queried item can be located
                if (cut.empty()) {
                    return false;
                }
            }
            return true;
        };
        
        for (auto &ops_item: ops) {
            diff_index.insert(ops_item[0], ops_item[1], ops_item[2], ops_item[3]);
            queries.emplace_back(ops_item[0], ops_item[1]);            
            ++count;
            
            if (count % 5000 == 0) {
                ASSERT_TRUE(run_queries());
            }
        }
    }
    
    void runQueryTestWithFile(const std::string &ops_filename, std::uint64_t test_page_num,
        std::uint32_t test_state_num)
    {
        // use page size same as BDevStorage (by default)
        auto dram_page_size = 16384u;
        auto dram_prefix = std::make_shared<DRAM_Prefix>(dram_page_size);
        auto dram_allocator = std::make_shared<DRAM_Allocator>(dram_page_size);        
        auto dram_pair = DRAM_Pair { dram_prefix, dram_allocator };
        
        auto ops = loadArray(ops_filename);

        Address addr;
        Address di_addr;
        {
            SparseIndex sparse_index(SparseIndex::tag_create(), dram_pair);
            DiffIndex diff_index(DiffIndex::tag_create(), dram_pair);            
            addr = sparse_index.getIndexAddress();
            di_addr = diff_index.getIndexAddress();
        }
        
        SparseIndex sparse_index(dram_pair, AccessType::READ_WRITE, addr);
        DiffIndex diff_index(dram_pair, AccessType::READ_WRITE, di_addr);
        for (auto &ops_item: ops) {
            if (ops_item[0] == 0) {
                sparse_index.emplace(ops_item[1], ops_item[2], ops_item[3]);
            } else {
                diff_index.insert(ops_item[1], ops_item[2], ops_item[3], ops_item[4]);
            }
        }
        
        SparseIndexQuery cut(sparse_index, diff_index, test_page_num, test_state_num);
        ASSERT_FALSE(cut.empty());
    }
    
    TEST_F( DiffIndexTest , DISABLED_testDiffIndexQueryIssue1 )
    {
        runQueryTestWithFile("./tests/files/sparse_pair_ops_2.csv", 1376800u, 3u);
    }
    
    TEST_F( DiffIndexTest , DISABLED_testDiffIndexQueryIssue2 )
    {
        runQueryTestWithFile("./tests/files/sparse_pair_ops.csv", 7110756u, 8u);
    }

}
