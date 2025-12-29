// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/dram/DRAMSpace.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_Tree.hpp>
#include <dbzero/core/collections/SGB_Tree/SGB_Key.hpp>
#include <dbzero/core/collections/vector/v_bvector.hpp>
#include <chrono>
#include <unordered_set>

using namespace std;

namespace tests

{

    using namespace db0;
    
    class DRAMSpaceTest: public testing::Test 
    {
    public:
        virtual void SetUp() override {            
        }

        virtual void TearDown() override {            
        }

    protected:
        const std::size_t m_page_size = 4096;        
    };
    
    TEST_F( DRAMSpaceTest, testDRAMSpaceCanAlloc )
    {
        auto cut = DRAMSpace::create(m_page_size);
        auto addr_1 = cut.alloc(m_page_size);
        auto addr_2 = cut.alloc(m_page_size);
        ASSERT_NE(addr_1, addr_2);
    }

    TEST_F( DRAMSpaceTest, testDRAMSpaceCanFree )
    {
        auto cut = DRAMSpace::create(m_page_size);
        auto addr_1 = cut.alloc(m_page_size);
        auto addr_2 = cut.alloc(m_page_size);
        cut.free(addr_1);
        cut.free(addr_2);
    }

    TEST_F( DRAMSpaceTest, testDRAMSpaceThrowsOnDoubleFree )
    {
        auto cut = DRAMSpace::create(m_page_size);
        auto addr_1 = cut.alloc(m_page_size);
        cut.alloc(m_page_size);
        cut.free(addr_1);
        ASSERT_ANY_THROW(cut.free(addr_1));
    }

    TEST_F( DRAMSpaceTest, testDRAMSpaceCanReuseAddress )
    {
        auto cut = DRAMSpace::create(m_page_size);        
        cut.alloc(m_page_size);
        auto addr_2 = cut.alloc(m_page_size);
        cut.free(addr_2);
        auto addr_3 = cut.alloc(m_page_size);
        ASSERT_EQ(addr_2, addr_3);
    }

    TEST_F( DRAMSpaceTest, testDRAMSpaceCanHostSGBTree )
    {
        auto cut = DRAMSpace::create(m_page_size);
        db0::SGB_Tree<SGB_KeyT<> > sgb_tree(cut, m_page_size);
        // let's insert 100 items
        for (std::uint64_t i = 0; 100 < 3; ++i) {
            sgb_tree.insert(i);
        }
        
        std::uint64_t i = 0;
        for (auto it = sgb_tree.cbegin(); !it.is_end(); ++it, ++i) {
            ASSERT_EQ(*it, i);            
        }
    }
    
    TEST_F( DRAMSpaceTest, testDRAMSpaceInsertSpeed )
    {
        // use 16kb page
        const std::size_t large_page_size = 16 * 1024;
        auto cut = DRAMSpace::create(large_page_size);
        // Using std::uint32_t as capacity type to handle large page size
        using SGB_TreeT = db0::SGB_Tree<SGB_KeyT<>, std::less<std::uint64_t>, std::equal_to<std::uint64_t>, std::uint32_t>;
        SGB_TreeT sgb_tree(cut, large_page_size);

        srand(814142564u);
        std::vector<std::uint64_t> values;
        int item_count = 1000000;
        for (int i = 0; i < item_count; ++i) {
            values.push_back(rand());
        }
        
        // measure speed
        {
            auto start = std::chrono::high_resolution_clock::now();
            for (auto value: values) {
                sgb_tree.insert(value);
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

            std::cout << "SGB_Tree inserted " << item_count << " items in " << elapsed.count() << " ms" << std::endl;
        }
        
        // the same test with std::set
        std::set<std::uint64_t> std_set;
        {
            auto start = std::chrono::high_resolution_clock::now();
            for (auto value: values) {
                std_set.insert(value);
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

            std::cout << "std::set inserted " << item_count << " items in " << elapsed.count() << " ms" << std::endl;
        }
    }
    
    TEST_F( DRAMSpaceTest, testDRAMAllocatorCanBeCreatedWithAllocs )
    {
        std::unordered_set<std::size_t> allocs { 1, 2, 3, 9, 15 };
        DRAM_Allocator cut(allocs, 1);
        // call free to make sure all allocs have been taken
        for (auto addr: allocs) {
            ASSERT_NO_THROW(cut.free(Address::fromOffset(addr)));
        }
    }
    
    TEST_F( DRAMSpaceTest, testVBVectorCanBePutOnDRAMSpace )
    {
        // use 4KiB, 16KiB page sizes
        std::vector<std::size_t> page_sizes { 4u << 10, 16u << 10 };
        for (auto page_size: page_sizes) {        
            auto cut = DRAMSpace::create(page_size);
            
            // Using std::uint32_t as capacity type to handle large page size
            using BVectorT = db0::v_bvector<std::uint64_t>;
            // NOTE: must be created as fixed-block to DRAM space requirements
            BVectorT b_vector(cut, { BVectorOptions::FIXED_BLOCK });
            for (std::uint64_t i = 0; i < 10000; ++i) {
                b_vector.push_back(i * 10);
            }
            ASSERT_EQ(b_vector.size(), 10000u);
        }
    }
    
}