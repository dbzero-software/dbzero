// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <utils/TestWorkspace.hpp>
#include <dbzero/core/storage/SparsePair.hpp>
#include <dbzero/core/storage/SparsePairManager.hpp>
#include <dbzero/core/dram/MetaPrefix.hpp>
#include <dbzero/core/dram/MetaSpace.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
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
        static constexpr std::size_t page_size = 4096;
        using DP_ChangeLogStreamT = SparsePair::DP_ChangeLogStreamT;

        SparsePairTest() = default;

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override {        
            drop(file_name);
        }

        static DRAM_Pair createMappingPair()
        {
            return {
                std::make_shared<db0::DRAM_Prefix>(page_size),
                std::make_shared<db0::DRAM_Allocator>(page_size)
            };
        }

        static Diff_IO createIO(CFile &file)
        {
            auto tail_function = [&file]() -> std::uint64_t {
                return file.size();
            };
            return Diff_IO(0, file, page_size, page_size * 16, page_size, 0, 1, tail_function, 0, 4);
        }

        static bool flushMeta(Memspace &memspace, Diff_IO &io)
        {
            return flush(dynamic_cast<MetaPrefix &>(memspace.getPrefix()), io);
        }

        static Allocator::SlotId addressSlotId(Address address)
        {
            return MS_MetaPrefix::slotIdFromPageNum(address.getOffset() / page_size);
        }
    };

    class SlotRecordingDRAMAllocator: public db0::DRAM_Allocator
    {
    public:
        explicit SlotRecordingDRAMAllocator(std::size_t page_size)
            : db0::DRAM_Allocator(page_size)
        {
        }

        std::optional<Address> tryAlloc(std::size_t size, SlotId slot_num,
            bool aligned = false, unsigned char realm_id = 0, unsigned char locality = 0) override
        {
            m_slot_records.push_back(slot_num);
            return DRAM_Allocator::tryAlloc(size, 0, aligned, realm_id, locality);
        }

        const std::vector<SlotId> &slotRecords() const {
            return m_slot_records;
        }

    private:
        std::vector<SlotId> m_slot_records;
    };

    TEST_F( SparsePairTest , testSparsePairAllocatesInternalStorageFromRequestedSlot )
    {
        constexpr std::size_t node_size = 4096;
        constexpr Allocator::SlotId slot_num = 7;
        auto prefix = std::make_shared<db0::DRAM_Prefix>(node_size);
        auto allocator = std::make_shared<SlotRecordingDRAMAllocator>(node_size);
        DRAM_Pair dram_pair { prefix, allocator };

        SparsePair cut(SparsePair::tag_create(), dram_pair, slot_num);
        ASSERT_GE(allocator->slotRecords().size(), 2u);
        ASSERT_TRUE(std::all_of(allocator->slotRecords().begin(), allocator->slotRecords().end(),
            [](Allocator::SlotId recorded_slot_num) {
                return recorded_slot_num == slot_num;
            }));

        for (std::uint64_t i = 1; i <= 300; ++i) {
            cut.getSparseIndex().emplace(i << 24, static_cast<std::uint32_t>(i), i + 1000);
            cut.getDiffIndex().insert((i + 1000) << 24, static_cast<std::uint32_t>(i), i + 2000);
        }

        auto allocation_count_after_growth = allocator->slotRecords().size();
        ASSERT_GT(allocation_count_after_growth, 2u);
        ASSERT_TRUE(std::all_of(allocator->slotRecords().begin(), allocator->slotRecords().end(),
            [](Allocator::SlotId recorded_slot_num) {
                return recorded_slot_num == slot_num;
            }));

        SparsePair reopened(dram_pair, AccessType::READ_WRITE, {}, slot_num);
        for (std::uint64_t i = 301; i <= 600; ++i) {
            reopened.getSparseIndex().emplace(i << 24, static_cast<std::uint32_t>(i), i + 1000);
            reopened.getDiffIndex().insert((i + 1000) << 24, static_cast<std::uint32_t>(i), i + 2000);
        }

        ASSERT_GT(allocator->slotRecords().size(), allocation_count_after_growth);
        ASSERT_TRUE(std::all_of(allocator->slotRecords().begin(), allocator->slotRecords().end(),
            [](Allocator::SlotId recorded_slot_num) {
                return recorded_slot_num == slot_num;
            }));
    }

    TEST_F( SparsePairTest , testSparsePairManagerCachesPairsBySparseSlotId )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);
        auto meta_space = MS_MetaSpace::create(page_size, meta_pair, io);
        SparsePairManager manager(meta_space);

        auto &slot_7_first = manager.getOrCreate(7);
        auto &slot_7_second = manager.getOrCreate(7);
        auto &slot_19 = manager.getOrCreate(19);

        ASSERT_EQ(&slot_7_first, &slot_7_second);
        ASSERT_EQ(&slot_7_first, manager.tryGetCached(7));
        ASSERT_NE(&slot_7_first, &slot_19);
        ASSERT_EQ(addressSlotId(slot_7_first.getSparseIndex().getIndexAddress()), 7u);
        ASSERT_EQ(addressSlotId(slot_7_first.getDiffIndex().getIndexAddress()), 7u);
        ASSERT_EQ(addressSlotId(slot_19.getSparseIndex().getIndexAddress()), 19u);
        ASSERT_EQ(addressSlotId(slot_19.getDiffIndex().getIndexAddress()), 19u);
    }

    TEST_F( SparsePairTest , testSparsePairManagerReopensExistingSlotPair )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);
        auto meta_space = MS_MetaSpace::create(page_size, meta_pair, io);

        {
            SparsePairManager manager(meta_space);
            auto &slot_pair = manager.getOrCreate(17);
            slot_pair.getSparseIndex().insert({ 42, 3, 77 });
            slot_pair.getDiffIndex().insert(43, 4, 78);
        }

        SparsePairManager reopened_manager(meta_space);
        auto &reopened_pair = reopened_manager.getOrCreate(17);
        auto sparse_item = reopened_pair.getSparseIndex().lookup(42, 3);

        ASSERT_TRUE(!!sparse_item);
        ASSERT_EQ(sparse_item.m_storage_page_num, 77u);
        ASSERT_EQ(reopened_pair.getDiffIndex().findLower(43, 4), 4u);
    }

    TEST_F( SparsePairTest , testSparsePairManagerReopensSlotPairAfterMetaSpaceFlush )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);

        {
            auto meta_space = MS_MetaSpace::create(page_size, meta_pair, io);
            SparsePairManager manager(meta_space);
            auto &slot_pair = manager.getOrCreate(23);
            slot_pair.getSparseIndex().insert({ 100, 5, 700 });
            ASSERT_TRUE(flushMeta(meta_space, io));
        }

        auto reopened_meta_space = MS_MetaSpace::create(page_size, meta_pair, io);
        SparsePairManager manager(reopened_meta_space);
        auto &reopened_pair = manager.getOrCreate(23);
        auto sparse_item = reopened_pair.getSparseIndex().lookup(100, 5);

        ASSERT_TRUE(!!sparse_item);
        ASSERT_EQ(sparse_item.m_storage_page_num, 700u);
    }
    
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
