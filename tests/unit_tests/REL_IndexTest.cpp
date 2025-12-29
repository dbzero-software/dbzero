// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <vector>
#include <utils/TestBase.hpp>
#include <dbzero/core/storage/REL_Index.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/dram/DRAMSpace.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <utils/utils.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class REL_IndexTest: public testing::Test
    {    
    };
    
    TEST_F( REL_IndexTest , testREL_IndexGetAbsolute )
    {
        auto node_size = 16u << 10;
        auto memspace = DRAMSpace::create(node_size);
        REL_Index cut(memspace, 16u << 10, AccessType::READ_WRITE);
        std::vector<REL_Item> items {
            // relative page number, absolute page number
            { 0, 100 }, { 50, 200 }, { 100, 300 }, { 150, 400 }, { 200, 500 }
        };

        for (auto &item: items) {
            cut.addMapping(item.m_storage_page_num, item.m_rel_page_num, 50);
        }

        // relative -> absolute queries
        std::vector<std::pair<std::uint64_t, std::uint64_t> > queries {
            { 13, 113 }, { 50, 200 }, { 75, 225 }, { 100, 300 }, { 125, 325 }, { 150, 400 }, 
            { 175, 425 }, { 200, 500 }, { 0, 100 }
        };

        for (auto &query: queries) {
            auto abs_page_num = cut.getAbsolute(query.first);
            ASSERT_EQ(abs_page_num, query.second)
                << "Relative page num: " << query.first;
        }
    }

    TEST_F( REL_IndexTest , testREL_IndexGetRelative )
    {
        auto node_size = 16u << 10;
        auto memspace = DRAMSpace::create(node_size);
        REL_Index cut(memspace, 16u << 10, AccessType::READ_WRITE);
        std::vector<REL_Item> items {
            // relative page number, absolute page number
            { 0, 100 }, { 50, 200 }, { 100, 300 }, { 150, 400 }, { 200, 500 }
        };

        for (auto &item: items) {
            cut.addMapping(item.m_storage_page_num, item.m_rel_page_num, 50);
        }
        
        // absolute -> relative queries
        std::vector<std::pair<std::uint64_t, std::uint64_t> > queries {            
            { 113, 13 }, { 200, 50 }, { 225, 75 }, { 300, 100 }, { 325, 125 }, { 400, 150 }, 
            { 425, 175 }, { 500, 200 }, { 100, 0 }
        };
        
        for (auto &query: queries) {
            auto rel_page_num = cut.getRelative(query.first);
            ASSERT_EQ(rel_page_num, query.second)
                << "Absolute page num: " << query.first;
        }
    }

    TEST_F( REL_IndexTest , testREL_IndexSortedIteration )
    {
        auto node_size = 16u << 10;
        auto memspace = DRAMSpace::create(node_size);
        REL_Index cut(memspace, 16u << 10, AccessType::READ_WRITE);
        std::vector<REL_Item> items {
            // relative page number, absolute page number
            { 0, 100 }, { 50, 200 }, { 60, 210 }, { 100, 300 }, { 150, 400 }, { 160, 410 }, { 200, 500 }
        };
        
        for (auto &item: items) {
            cut.addMapping(item.m_storage_page_num, item.m_rel_page_num, 10);
        }
        
        std::vector<std::uint64_t> rel_page_nums;
        auto it = cut.cbegin();
        while (!it.is_end()) {
            rel_page_nums.push_back((*it).m_rel_page_num);
            ++it;
        }
    }

    TEST_F( REL_IndexTest , testREL_IndexIteratorIssue1 )
    {
        auto node_size = 16u << 10;
        auto memspace = DRAMSpace::create(node_size);
        REL_Index cut(memspace, 16u << 10, AccessType::READ_WRITE);
        // storage page num, relative page num, count
        cut.addMapping(32, 0, 14);
        cut.addMapping(64, 14, 64);
        cut.assignRelative(128, true);
        cut.assignRelative(144, true);
        
        std::vector<std::uint64_t> rel_page_nums;
        auto it = cut.cbegin();
        while (!it.is_end()) {            
            rel_page_nums.push_back((*it).m_storage_page_num);
            ++it;
        }
        ASSERT_EQ(rel_page_nums, (std::vector<std::uint64_t>{32, 64, 128, 144}));
    }

}
