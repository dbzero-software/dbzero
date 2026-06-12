// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/storage/SparseIndex.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/storage/ChangeLogIOStream.hpp>
#include <utils/utils.hpp>

using namespace std;
using namespace db0;
using namespace db0::tests;

namespace tests

{

    class SparseIndexTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-prefix_1.db0";
        SparseIndexTest() = default;

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override {        
            drop(file_name);
        }

        template <typename SparseIndexT = SparseIndex>
        static SparseIndexT createSparseIndex(std::size_t node_size,
            std::vector<std::uint64_t> *change_log = nullptr)
        {
            DRAM_Pair dram_pair {
                std::make_shared<DRAM_Prefix>(node_size),
                std::make_shared<DRAM_Allocator>(node_size)
            };
            return SparseIndexT(typename SparseIndexT::tag_create(), dram_pair, change_log);
        }
    };

    TEST_F( SparseIndexTest , testSparseIndexCanBeInstantiated ) {
        auto cut = createSparseIndex(16 * 1024);
    }

    TEST_F( SparseIndexTest , testSparseIndexBaseCanUseEmptyHeaderMixin )
    {
        using EmptySparseIndexBase = SparseIndexBase<SI_Item, SI_CompressedItem, EmptyMixin>;
        auto cut = createSparseIndex<EmptySparseIndexBase>(16 * 1024);

        cut.emplace(1, 1, 10);
        cut.emplace(1, 3, 30);
        cut.update(1, 4, 40);
        cut.modifyMixIn().refresh();

        ASSERT_FALSE(cut.lookup(1, 1));
        ASSERT_FALSE(cut.lookup(1, 3));
        auto updated = cut.lookup(1, 4);
        ASSERT_TRUE(updated);
        ASSERT_EQ(updated.m_storage_page_num, 40u);
    }

    TEST_F( SparseIndexTest , testSparseIndexCanAppendPageDescriptors )
    {
        auto cut = createSparseIndex(16 * 1024);
        cut.emplace(0, 0, 0);
    }

    void testSparseIndexLookupPageDescriptors(std::size_t node_size)
    {
        auto cut = SparseIndexTest::createSparseIndex(node_size);
        std::vector<typename SparseIndex::SI_ItemT> items {
            // page number, state number, physical page number, page type
            { 0, 1, 0 }, { 1, 1, 1 }, { 2, 1, 2 }, { 3, 2, 3 }, { 0, 2, 4 }, { 2, 3, 5 }, { 4, 4, 6 }
        };

        // page number, state number
        std::vector<std::pair<std::uint64_t, std::uint32_t> > queries {
            { 0, 1 }, { 0, 2 }, { 0, 3 },
            { 1, 1 }, { 1, 2 }, { 1, 3 }, { 1, 4 }, 
            { 2, 1 }, { 2, 2 }, { 2, 3 }, { 2, 4 }, 
            { 3, 1 }, { 3, 2 }, { 3, 3 }, { 3, 4 }, { 3, 5 },
            { 4, 1 }, { 4, 2 }, { 4, 3 }, { 4, 4 }, { 4, 5 }
        };
        
        // storage page number
        std::vector<std::optional<std::uint64_t> > m_expected_results {
            0, 4, 4,
            1, 1, 1, 1, 
            2, 2, 5, 5, 
            std::nullopt, 3, 3, 3, 3,
            std::nullopt, std::nullopt, std::nullopt, 6, 6
        };

        for (auto &item: items) {
            cut.insert(item);
        }
        unsigned int i = 0;
        for (auto &query: queries) {
            auto pd = cut.lookup(query);
            if (pd) {
                ASSERT_EQ(pd.m_storage_page_num, *m_expected_results[i]);
            } else {
                ASSERT_FALSE(m_expected_results[i].has_value());
            }
            ++i;
        }
    }

    TEST_F( SparseIndexTest , testSparseIndexLookupPageDescriptors )
    { 
        // also test with non-standard node size
        testSparseIndexLookupPageDescriptors(16 * 1024 - 256);
        testSparseIndexLookupPageDescriptors(16 * 1024);
    }

    TEST_F( SparseIndexTest , testSparseIndexOwnerCanRecordNextStoragePageNum )
    {
        auto cut = createSparseIndex(16 * 1024);
        cut.emplace(4, 3, 6);
        ASSERT_EQ(cut.mixIn().getNextStoragePageNum(), std::nullopt);
        cut.modifyMixIn().recordNextStoragePageNum(7);
        ASSERT_EQ(cut.mixIn().getNextStoragePageNum(), 7);
    }
    
    TEST_F( SparseIndexTest , testSparseIndexOwnerCanRecordMaxStateNum )
    {
        auto cut = createSparseIndex(16 * 1024);
        cut.emplace(4, 3, 6);
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 0);
        cut.modifyMixIn().recordMaxStateNum(3);
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 3);
    }

    TEST_F( SparseIndexTest , testSparseIndexUpdateReplacesOlderPageDescriptors )
    {
        auto cut = createSparseIndex(16 * 1024);
        cut.emplace(1, 1, 10);
        cut.emplace(1, 3, 30);
        cut.emplace(2, 2, 20);

        cut.update(1, 4, 40);

        ASSERT_FALSE(cut.lookup(1, 1));
        ASSERT_FALSE(cut.lookup(1, 3));
        auto updated = cut.lookup(1, 4);
        ASSERT_TRUE(updated);
        ASSERT_EQ(updated.m_storage_page_num, 40u);
        ASSERT_TRUE(cut.lookup(2, 2));
        ASSERT_EQ(cut.mixIn().getNextStoragePageNum(), std::nullopt);
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 0);
    }

    TEST_F( SparseIndexTest , testSparseIndexCanBeUpdatedByDRAMSpaceSwap )
    {   
        std::size_t node_size = 16 * 1024;     
        auto sparse_index = createSparseIndex(node_size);
        DRAM_Pair dram_pair;
        auto dram_space = DRAMSpace::create(node_size, [&](DRAM_Pair dp) {
            dram_pair = dp;
        });
        
        SparseIndex cut(SparseIndex::tag_create(), dram_pair);
        std::vector<typename SparseIndex::SI_ItemT> items_1 {
            // page number, state number, physical page number, page type
            { 0, 0, 0 }, { 1, 0, 1 }
        };

        for (auto &item: items_1) {
            sparse_index.insert(item);
        }
        // copy DRAM binary contents between the instances
        *(dram_pair.first) = sparse_index.getDRAMPrefix();

        // make sure the contents is in-sync
        ASSERT_EQ(cut.lookup(0, 0), sparse_index.lookup(0, 0));
        ASSERT_EQ(cut.lookup(1, 0), sparse_index.lookup(1, 0));

        std::vector<typename SparseIndex::SI_ItemT> items_2 {
            // page number, state number, physical page number, page type
            { 2, 0, 2 }, { 3, 1, 3 }, { 0, 1, 4 }, { 2, 2, 5 }, { 4, 3, 6 }
        };

        for (auto &item: items_2) {
            sparse_index.insert(item);        
        }

        (*dram_pair.first) = sparse_index.getDRAMPrefix();
        // make sure the contents is in-sync
        for (unsigned int i = 0; i < 5; ++i) {
            ASSERT_EQ(cut.lookup(i, 3), sparse_index.lookup(i, 3));
        }
    }

    TEST_F( SparseIndexTest , testSparseIndexMaxStateNumUpdatedAfterRefresh )
    {   
        std::size_t node_size = 16 * 1024;     
        auto sparse_index = createSparseIndex(node_size);
        DRAM_Pair dram_pair;
        auto dram_space = DRAMSpace::create(node_size, [&](DRAM_Pair dp) {
            dram_pair = dp;
        });

        SparseIndex cut(SparseIndex::tag_create(), dram_pair);
        std::vector<typename SparseIndex::SI_ItemT> items_1 {
            // page number, state number, physical page number, page type
            { 0, 0, 0 }, { 1, 1, 1 }
        };

        for (auto &item: items_1) {
            sparse_index.insert(item);
        }
        sparse_index.modifyMixIn().recordMaxStateNum(1);
        // copy DRAM binary contents between the instances
        *(dram_pair.first) = sparse_index.getDRAMPrefix();
        
        // make sure max-state-number reported correctly after refresh
        cut.refresh();
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 1);

        std::vector<typename SparseIndex::SI_ItemT> items_2 {
            // page number, state number, physical page number, page type
            { 2, 0, 2 }, { 3, 1, 3 }, { 0, 1, 4 }, { 2, 2, 5 }, { 4, 3, 6 }
        };

        for (auto &item: items_2) {
            sparse_index.insert(item);
        }
        sparse_index.modifyMixIn().recordMaxStateNum(3);

        (*dram_pair.first) = sparse_index.getDRAMPrefix();
        cut.refresh();
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 3);
    }
            
    TEST_F( SparseIndexTest , testSparseIndexInsertFailingCase )
    {
        auto cut = createSparseIndex(16 * 1024);
        std::vector<typename SparseIndex::SI_ItemT> items {
            // page number, state number, physical page number, page type
            { 0, 1, 0 }
        };
        for (auto &item: items) {
            cut.insert(item);
        }

        std::vector<std::uint64_t> page_num;
        cut.forAll([&](const typename SparseIndex::SI_ItemT &item) {
            page_num.push_back(item.m_page_num);        
        });
        ASSERT_EQ(page_num, std::vector<std::uint64_t> { 0 });
    }
    
    TEST_F( SparseIndexTest , testSparseIndexInsertLookupFailingCase )
    {
        auto cut = createSparseIndex(16 * 1024);
        std::vector<typename SparseIndex::SI_ItemT> items {
            // page number, state number, physical page number, page type
            { 0, 1, 0 }
        };
        for (auto &item: items) {
            cut.insert(item);
        }

        ASSERT_TRUE(cut.lookup(0, 1));
    }

    TEST_F( SparseIndexTest , testSparseIndexCanEraseExactPageState )
    {
        auto cut = createSparseIndex(16 * 1024);
        cut.emplace(1, 1, 10);
        cut.emplace(1, 3, 30);
        cut.emplace(2, 1, 20);

        ASSERT_TRUE(cut.erase(1, 3));
        ASSERT_FALSE(cut.erase(1, 3));
        ASSERT_EQ(cut.size(), 2u);
        ASSERT_EQ(cut.lookup(1, 3).m_storage_page_num, 10u);
        ASSERT_EQ(cut.lookup(2, 1).m_storage_page_num, 20u);
    }

    TEST_F( SparseIndexTest , testSparseIndexEraseBelowKeepsThresholdState )
    {
        auto cut = createSparseIndex(16 * 1024);
        cut.emplace(1, 1, 10);
        cut.emplace(1, 3, 30);
        cut.emplace(1, 5, 50);
        cut.emplace(2, 1, 20);

        ASSERT_EQ(cut.eraseBelow(1, 3), 1u);
        ASSERT_FALSE(cut.lookup(1, 1));
        ASSERT_EQ(cut.lookup(1, 3).m_storage_page_num, 30u);
        ASSERT_EQ(cut.lookup(1, 5).m_storage_page_num, 50u);
        ASSERT_EQ(cut.lookup(2, 5).m_storage_page_num, 20u);
    }

    TEST_F( SparseIndexTest , testSparseIndexEraseBelowNoOpCases )
    {
        auto cut = createSparseIndex(16 * 1024);
        cut.emplace(1, 1, 10);
        cut.emplace(1, 3, 30);

        ASSERT_EQ(cut.eraseBelow(1, 0), 0u);
        ASSERT_EQ(cut.eraseBelow(1, 1), 0u);
        ASSERT_EQ(cut.eraseBelow(2, 5), 0u);
        ASSERT_EQ(cut.size(), 2u);
    }

    TEST_F( SparseIndexTest , testSparseIndexEraseBelowCanEraseAcrossNodes )
    {
        auto cut = createSparseIndex(256);
        for (std::uint32_t state_num = 1; state_num <= 200; ++state_num) {
            cut.emplace(1, state_num, state_num);
            cut.emplace(2, state_num, 1000 + state_num);
        }

        ASSERT_EQ(cut.eraseBelow(1, 150), 149u);
        ASSERT_FALSE(cut.lookup(1, 149));
        ASSERT_EQ(cut.lookup(1, 150).m_storage_page_num, 150u);
        ASSERT_EQ(cut.lookup(2, 149).m_storage_page_num, 1149u);
    }

    TEST_F( SparseIndexTest , testSparseIndexEraseRangeSupportsOptionalBounds )
    {
        auto cut = createSparseIndex(256);
        for (std::uint32_t state_num = 1; state_num <= 20; ++state_num) {
            cut.emplace(1, state_num, state_num);
            cut.emplace(2, state_num, 1000 + state_num);
            cut.emplace(3, state_num, 2000 + state_num);
        }
        ASSERT_EQ(cut.size(), 60u);

        ASSERT_EQ(cut.eraseRange(1, 5, 10), 5u);
        ASSERT_EQ(cut.size(), 55u);
        ASSERT_EQ(cut.lookup(1, 4).m_storage_page_num, 4u);
        ASSERT_EQ(cut.lookup(1, 5).m_state_num, 4u);
        ASSERT_EQ(cut.lookup(1, 9).m_state_num, 4u);
        ASSERT_EQ(cut.lookup(1, 10).m_storage_page_num, 10u);
        ASSERT_EQ(cut.lookup(2, 9).m_storage_page_num, 1009u);

        ASSERT_EQ(cut.eraseRange(1, {}, 4), 3u);
        ASSERT_EQ(cut.size(), 52u);
        ASSERT_EQ(cut.lookup(1, 4).m_storage_page_num, 4u);

        ASSERT_EQ(cut.eraseRange(2, 18, {}), 3u);
        ASSERT_EQ(cut.size(), 49u);
        ASSERT_EQ(cut.lookup(2, 17).m_storage_page_num, 1017u);
        ASSERT_EQ(cut.lookup(2, 18).m_state_num, 17u);
        ASSERT_EQ(cut.lookup(2, 20).m_state_num, 17u);
        ASSERT_EQ(cut.lookup(3, 20).m_storage_page_num, 2020u);

        ASSERT_EQ(cut.eraseRange(3), 20u);
        ASSERT_EQ(cut.size(), 29u);
        ASSERT_FALSE(cut.lookup(3, 20));
        ASSERT_EQ(cut.lookup(2, 17).m_storage_page_num, 1017u);
    }

    TEST_F( SparseIndexTest , testSparseIndexEraseRangeNoOpCases )
    {
        auto cut = createSparseIndex(16 * 1024);
        cut.emplace(1, 1, 10);
        cut.emplace(1, 3, 30);

        ASSERT_EQ(cut.eraseRange(1, 1, 1), 0u);
        ASSERT_EQ(cut.eraseRange(1, 3, 1), 0u);
        ASSERT_EQ(cut.eraseRange(2), 0u);
        ASSERT_EQ(cut.size(), 2u);
    }

    TEST_F( SparseIndexTest , testSparseIndexEraseRangeLowerOnlyAtMaxPage )
    {
        auto cut = createSparseIndex(16 * 1024);
        constexpr auto page_num = (static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) << 24) | 0xFFFFFFu;
        constexpr auto max_state_num = std::numeric_limits<std::uint32_t>::max();
        cut.emplace(page_num, 1, 10);
        cut.emplace(page_num, max_state_num, 20);
        cut.emplace(page_num - 1, max_state_num, 30);

        ASSERT_EQ(cut.eraseRange(page_num, max_state_num, {}), 1u);
        ASSERT_EQ(cut.lookup(page_num, 1).m_storage_page_num, 10u);
        ASSERT_EQ(cut.lookup(page_num, max_state_num).m_state_num, 1u);
        ASSERT_EQ(cut.lookup(page_num - 1, max_state_num).m_storage_page_num, 30u);
    }

    TEST_F( SparseIndexTest , testSparseIndexEraseBelowEdgeCasesWithSmallNodes )
    {
        auto cut = createSparseIndex(192);
        for (std::uint32_t state_num = 1; state_num <= 80; ++state_num) {
            cut.emplace(1, state_num, state_num);
            cut.emplace(2, state_num, 1000 + state_num);
        }
        ASSERT_EQ(cut.size(), 160u);

        ASSERT_EQ(cut.eraseBelow(1, 1), 0u);
        ASSERT_EQ(cut.eraseBelow(1, 0), 0u);
        ASSERT_EQ(cut.eraseBelow(99, 50), 0u);
        ASSERT_EQ(cut.size(), 160u);

        ASSERT_EQ(cut.eraseBelow(1, 41), 40u);
        ASSERT_FALSE(cut.lookup(1, 40));
        ASSERT_EQ(cut.lookup(1, 41).m_storage_page_num, 41u);
        ASSERT_EQ(cut.lookup(2, 40).m_storage_page_num, 1040u);
        ASSERT_EQ(cut.size(), 120u);

        ASSERT_EQ(cut.eraseBelow(1, 41), 0u);
        ASSERT_TRUE(cut.erase(1, 41));
        ASSERT_FALSE(cut.lookup(1, 41));
        ASSERT_EQ(cut.lookup(1, 42).m_storage_page_num, 42u);
        ASSERT_EQ(cut.size(), 119u);

        ASSERT_EQ(cut.eraseBelow(1, std::numeric_limits<std::uint32_t>::max()), 39u);
        ASSERT_FALSE(cut.lookup(1, 100));
        ASSERT_EQ(cut.lookup(2, 80).m_storage_page_num, 1080u);
        ASSERT_EQ(cut.size(), 80u);

        ASSERT_EQ(cut.eraseBelow(2, std::numeric_limits<std::uint32_t>::max()), 80u);
        ASSERT_TRUE(cut.empty());
    }

    TEST_F( SparseIndexTest , testSparseIndexEraseDoesNotRecordChangeLog )
    {
        std::vector<std::uint64_t> change_log;
        auto cut = createSparseIndex(16 * 1024, &change_log);
        cut.emplace(1, 1, 10);
        cut.emplace(1, 2, 20);
        cut.emplace(1, 3, 30);
        change_log.clear();

        ASSERT_EQ(cut.eraseBelow(1, 3), 2u);
        ASSERT_TRUE(change_log.empty());

        change_log.clear();
        ASSERT_TRUE(cut.erase(1, 3));
        ASSERT_TRUE(change_log.empty());

        change_log.clear();
        ASSERT_FALSE(cut.erase(1, 3));
        ASSERT_TRUE(change_log.empty());

        ASSERT_EQ(cut.eraseRange(1), 0u);
        ASSERT_TRUE(change_log.empty());
    }

    TEST_F( SparseIndexTest , testSparseIndexClearRemovesAllDescriptorsAndPreservesCounters )
    {
        auto cut = createSparseIndex(192);
        for (std::uint32_t state_num = 1; state_num <= 80; ++state_num) {
            cut.emplace(1, state_num, state_num);
            cut.emplace(2, state_num, 1000 + state_num);
        }
        cut.modifyMixIn().recordNextStoragePageNum(1081);
        cut.modifyMixIn().recordMaxStateNum(80);
        ASSERT_GT(cut.size(), 2u);
        ASSERT_EQ(cut.mixIn().getNextStoragePageNum(), 1081u);
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 80u);

        cut.clear();

        ASSERT_TRUE(cut.empty());
        ASSERT_EQ(cut.size(), 0u);
        ASSERT_EQ(cut.mixIn().getNextStoragePageNum(), 1081u);
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 80u);
        ASSERT_FALSE(cut.lookup(1, 80));
        ASSERT_FALSE(cut.lookup(2, 80));

        cut.emplace(3, 81, 0);
        ASSERT_EQ(cut.size(), 1u);
        ASSERT_EQ(cut.lookup(3, 81).m_storage_page_num, 0u);
        ASSERT_EQ(cut.mixIn().getNextStoragePageNum(), 1081u);
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 80u);
    }

    TEST_F( SparseIndexTest , testSparseIndexClearEmptyAndChangeLogNoOp )
    {
        std::vector<std::uint64_t> change_log;
        auto cut = createSparseIndex(16 * 1024, &change_log);

        cut.clear();
        ASSERT_TRUE(cut.empty());
        ASSERT_EQ(cut.size(), 0u);
        ASSERT_TRUE(change_log.empty());

        cut.emplace(1, 1, 10);
        ASSERT_FALSE(change_log.empty());
        change_log.clear();

        cut.clear();
        ASSERT_TRUE(cut.empty());
        ASSERT_EQ(cut.size(), 0u);
        ASSERT_TRUE(change_log.empty());
        ASSERT_EQ(cut.mixIn().getNextStoragePageNum(), std::nullopt);
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 0u);

        cut.emplace(2, 2, 0);
        ASSERT_EQ(cut.mixIn().getNextStoragePageNum(), std::nullopt);
        ASSERT_EQ(cut.mixIn().getMaxStateNum(), 0u);
    }

    TEST_F( SparseIndexTest , testSparseIndexForPageRangeUsesHalfOpenBounds )
    {
        auto cut = createSparseIndex(16 * 1024);
        constexpr std::uint64_t slot_size = 1ull << 24;
        constexpr std::uint64_t slot_1_first = slot_size;
        constexpr std::uint64_t slot_2_first = slot_size * 2;

        cut.emplace(slot_1_first - 1, 1, 10);
        cut.emplace(slot_1_first, 1, 20);
        cut.emplace(slot_1_first + 7, 2, 21);
        cut.emplace(slot_2_first, 1, 30);

        std::vector<std::uint64_t> page_nums;
        cut.forPageRange(slot_1_first, slot_2_first, [&](const SI_Item &item) {
            page_nums.push_back(item.m_page_num);
        });

        ASSERT_EQ(page_nums, (std::vector<std::uint64_t> { slot_1_first, slot_1_first + 7 }));
    }

    TEST_F( SparseIndexTest , testSparseIndexForPageRangeHandlesEmptyAndOutOfRangeScans )
    {
        auto empty_cut = createSparseIndex(16 * 1024);
        std::size_t callback_count = 0;
        empty_cut.forPageRange(1, 10, [&](const SI_Item &) {
            ++callback_count;
        });
        ASSERT_EQ(callback_count, 0u);

        auto cut = createSparseIndex(16 * 1024);
        cut.emplace(100, 1, 10);
        cut.emplace(200, 1, 20);

        cut.forPageRange(10, 10, [&](const SI_Item &) {
            ++callback_count;
        });
        cut.forPageRange(10, 20, [&](const SI_Item &) {
            ++callback_count;
        });
        ASSERT_EQ(callback_count, 0u);
    }

    TEST_F( SparseIndexTest , testSparseIndexForPageRangeScansAcrossMultipleNodes )
    {
        auto cut = createSparseIndex(512);
        for (std::uint64_t page_num = 0; page_num < 200; ++page_num) {
            cut.emplace(page_num, 1, page_num + 1000);
        }

        std::vector<std::uint64_t> page_nums;
        cut.forPageRange(40, 75, [&](const SI_Item &item) {
            page_nums.push_back(item.m_page_num);
        });

        ASSERT_EQ(page_nums.size(), 35u);
        ASSERT_EQ(page_nums.front(), 40u);
        ASSERT_EQ(page_nums.back(), 74u);
    }
        
}
