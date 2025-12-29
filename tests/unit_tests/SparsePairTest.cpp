// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/storage/SparsePair.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <utils/utils.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class SparsePairTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-prefix_1.db0";
        using DP_ChangeLogStreamT = SparsePair::DP_ChangeLogStreamT;

        SparsePairTest() = default;

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override {        
            drop(file_name);
        }
    };
    
    TEST_F( SparsePairTest , testSparsePairCollectsChangeLogOfAddedItems )
    {   
        std::size_t node_size = 16 * 1024;
        SparsePair sparse_pair(node_size);        
        DRAM_Pair dram_pair;
        auto dram_space = DRAMSpace::create(node_size, [&](DRAM_Pair dp) {
            dram_pair = dp;
        });

        SparsePair cut(SparsePair::tag_create(), dram_pair);
        auto &sparse_index = cut.getSparseIndex();
        std::vector<typename SparseIndex::SI_ItemT> items_1 {
            // page number, state number, physical page number
            { 1, 1, 1 }, { 0, 1, 0 }
        };

        for (auto &item: items_1) {
            sparse_index.insert(item);
        }

        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&]() {
            return file.size();
        };

        {
            DP_ChangeLogStreamT io(file, 0, 4096, tail_function);
            auto &change_log = cut.extractChangeLog(io, 0);
            std::vector<std::uint64_t> data;
            for (auto value: change_log) {
                data.push_back(value);
            }
            io.close();            
            ASSERT_EQ(data, (std::vector<std::uint64_t> { 0, 1 }));
            ASSERT_EQ(change_log.m_state_num, 1u);
        }
        
        std::vector<typename SparseIndex::SI_ItemT> items_2 {
            // page number, state number, physical page number
            { 2, 1, 2 }, { 3, 2, 3 }, { 0, 3, 4 }, { 2, 4, 5 }, { 4, 5, 6 }
        };

        for (auto &item: items_2) {
            sparse_index.insert(item);
        }
        
        {
            DP_ChangeLogStreamT io(file, 0, 4096, tail_function);
            while (io.readChangeLogChunk());
            auto &change_log = cut.extractChangeLog(io, 0);
            std::vector<std::uint64_t> expected_data { 0, 2, 3, 4 };
            std::vector<std::uint64_t> data;
            for (auto value: change_log) {
                data.push_back(value);
            }
            io.close();
            ASSERT_EQ(data, expected_data);
            ASSERT_EQ(change_log.m_state_num, 5u);
        }
    }

    TEST_F( SparsePairTest , testSparseIndexBadWriteIssue )
    {
        // note non-standard page size (used in production)
        std::size_t dp_size = 16356;
        auto prefix = std::make_shared<db0::DRAM_Prefix>(dp_size);
        auto allocator = std::make_shared<db0::DRAM_Allocator>(dp_size);
        
        CFile::create(file_name, {});
        db0::CFile file(file_name, AccessType::READ_WRITE);
        auto tail_function = [&]() {
            return file.size();
        };        
        
        {
            // create an empty instance
            SparsePair cut(SparsePair::tag_create(), { prefix, allocator});
        }

        int count = 10;
        for (int i = 0; i < count; ++i) {
            SparsePair cut({ prefix, allocator}, AccessType::READ_WRITE);
            auto &sparse_index = cut.getSparseIndex();
            for (unsigned int page_num = 0; page_num < 1000; ++page_num) {
                sparse_index.emplace(page_num, i, 999);
            }
            
            // simulate change log extraction
            DP_ChangeLogStreamT io(file, 0, 16 << 10, tail_function, AccessType::READ_WRITE);
            while (io.readChangeLogChunk());
            cut.extractChangeLog(io, 0);
            io.close();

            // refresh updates local cached variables with DRAM prefix
            cut.refresh();
            ASSERT_EQ(cut.getMaxStateNum(), i);
        }
    }
    
}
