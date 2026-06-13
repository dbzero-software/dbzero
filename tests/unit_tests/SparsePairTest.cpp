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
#include <dbzero/core/dram/MS_Address.hpp>
#include <dbzero/core/dram/MS_MetaAllocator.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
#include <dbzero/core/storage/RandomIO_Stream.hpp>
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
            return Diff_IO(0, file, page_size, page_size * 16, page_size, 0, 1, tail_function, 0);
        }

        static RandomIO_Stream createStream(Diff_IO &io)
        {
            return RandomIO_Stream(io, 2);
        }

        static bool flushMeta(Memspace &memspace, RandomIO_Stream &io, SparsePair &sparse_pair)
        {
            auto &prefix = dynamic_cast<MetaPrefix &>(memspace.getPrefix());
            if (prefix.getDirtySize() != 0) {
                sparse_pair.recordMaxStateNum(prefix.getStateNum(false) + 1);
            }
            return flush(prefix, io);
        }

        static Allocator::SlotId addressSlotId(Address address)
        {
            return MS_Address::from(address.getOffset()).slot_id();
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

        SparsePair reopened(dram_pair, AccessType::READ_WRITE, {}, {}, slot_num);
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
        auto stream = createStream(io);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);
        auto meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);
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

    TEST_F( SparsePairTest , testSparsePairCanUseExternalChangeLog )
    {
        SparsePair::ChangeLogT change_log;
        auto dram_pair = createMappingPair();
        SparsePair cut(SparsePair::tag_create(), dram_pair, 0, &change_log);

        cut.getSparseIndex().emplace(11, 1, 100);
        cut.getDiffIndex().insert(12, 2, 101);

        ASSERT_EQ(cut.getChangeLogSize(), 2u);
        ASSERT_EQ(change_log, (SparsePair::ChangeLogT { 11, 12 }));
    }

    TEST_F( SparsePairTest , testSparsePairManagerUsesSharedChangeLog )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);
        auto meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);
        SparsePairManager manager(meta_space);

        auto &slot_7 = manager.getOrCreate(7);
        auto &slot_19 = manager.getOrCreate(19);
        slot_7.getSparseIndex().emplace(11, 1, 100);
        slot_19.getDiffIndex().insert(12, 2, 101);

        ASSERT_EQ(manager.getChangeLogSize(), 2u);
        auto page_nums = manager.extractChangeLogPages();
        ASSERT_EQ(page_nums, (std::vector<std::uint64_t> {
            MS_Address::encode(7, 11),
            MS_Address::encode(19, 12)
        }));
        ASSERT_EQ(manager.getChangeLogSize(), 0u);
    }

    TEST_F( SparsePairTest , testSparsePairManagerCommitOnlyUsesDirtyCachedPairs )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);
        auto meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);
        SparsePairManager manager(meta_space);

        auto &dirty_slot = manager.getOrCreate(7);
        auto &other_dirty_slot = manager.getOrCreate(19);
        auto &clean_slot = manager.getOrCreate(31);
        dirty_slot.getSparseIndex().emplace(11, 1, 100);
        other_dirty_slot.getSparseIndex().emplace(13, 1, 102);
        dirty_slot.getDiffIndex().insert(12, 2, 101);

        manager.commit();

        ASSERT_EQ(manager.getChangeLogSize(), 3u);
        auto page_nums = manager.extractChangeLogPages();
        ASSERT_EQ(page_nums, (std::vector<std::uint64_t> {
            MS_Address::encode(7, 11),
            MS_Address::encode(19, 13),
            MS_Address::encode(7, 12)
        }));
        ASSERT_TRUE(!!dirty_slot.getSparseIndex().lookup(11, 1));
        ASSERT_TRUE(!!other_dirty_slot.getSparseIndex().lookup(13, 1));
        ASSERT_TRUE(clean_slot.empty());
    }

    TEST_F( SparsePairTest , testSparsePairManagerRefreshesAffectedSlotInPlace )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);
        auto meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);
        SparsePairManager manager(meta_space);

        auto &slot_7 = manager.getOrCreate(7);
        auto &slot_19 = manager.getOrCreate(19);
        slot_7.getSparseIndex().insert({ 11, 1, 100 });
        manager.commit();
        ASSERT_TRUE(flushMeta(meta_space, stream, meta_pair));

        auto *slot_7_before = &slot_7;
        auto *slot_19_before = &slot_19;
        manager.refreshPages({
            MS_Address::encode(7, 11),
            MS_Address::encode(7, 11)
        });

        ASSERT_EQ(manager.tryGetCached(7), slot_7_before);
        ASSERT_EQ(manager.tryGetCached(19), slot_19_before);
        ASSERT_EQ(manager.tryGetExisting(7), slot_7_before);
        ASSERT_EQ(manager.tryGetCached(7), slot_7_before);
    }

    TEST_F( SparsePairTest , testSparsePairManagerEvictsSlot )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);
        auto meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);
        SparsePairManager manager(meta_space);

        auto &slot_7 = manager.getOrCreate(7);
        auto &slot_19 = manager.getOrCreate(19);
        slot_7.getSparseIndex().insert({ 11, 1, 100 });
        slot_19.getSparseIndex().insert({ 12, 1, 101 });
        manager.commit();
        ASSERT_TRUE(flushMeta(meta_space, stream, meta_pair));

        manager.evictSlot(7);

        ASSERT_EQ(manager.tryGetCached(7), nullptr);
        auto &reopened_slot_7 = manager.getOrCreate(7);
        auto *reopened_slot_7_ptr = &reopened_slot_7;
        auto *slot_19_ptr = &slot_19;

        manager.refreshPages({
            MS_Address::encode(7, 11),
            MS_Address::encode(19, 12)
        });

        ASSERT_EQ(manager.tryGetCached(7), reopened_slot_7_ptr);
        ASSERT_EQ(manager.tryGetExisting(7), reopened_slot_7_ptr);
        ASSERT_EQ(manager.tryGetCached(7), reopened_slot_7_ptr);
        ASSERT_EQ(manager.tryGetCached(19), slot_19_ptr);
        ASSERT_EQ(manager.tryGetExisting(19), slot_19_ptr);
        ASSERT_EQ(manager.tryGetCached(19), slot_19_ptr);
    }

    TEST_F( SparsePairTest , testSparsePairManagerOpensExistingSlotPair )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);
        auto meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);

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

    TEST_F( SparsePairTest , testSparsePairManagerOpensSlotPairAfterMetaSpaceFlush )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);

        {
            auto meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);
            SparsePairManager manager(meta_space);
            auto &slot_pair = manager.getOrCreate(23);
            slot_pair.getSparseIndex().insert({ 100, 5, 700 });
            ASSERT_TRUE(flushMeta(meta_space, stream, meta_pair));
        }

        auto reopened_meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);
        SparsePairManager manager(reopened_meta_space);
        auto &reopened_pair = manager.getOrCreate(23);
        auto sparse_item = reopened_pair.getSparseIndex().lookup(100, 5);

        ASSERT_TRUE(!!sparse_item);
        ASSERT_EQ(sparse_item.m_storage_page_num, 700u);
    }

    TEST_F( SparsePairTest , testSparsePairManagerRefreshSeesSlotCreatedAfterMiss )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        auto mapping_pair = createMappingPair();
        SparsePair meta_pair(SparsePair::tag_create(), mapping_pair);

        auto writer_meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);
        auto reader_meta_space = MS_MetaSpace::create(page_size, meta_pair, stream);
        SparsePairManager reader_manager(reader_meta_space);

        ASSERT_EQ(reader_manager.tryGetExisting(0), nullptr);

        {
            SparsePairManager writer_manager(writer_meta_space);
            auto &slot_pair = writer_manager.getOrCreate(0);
            slot_pair.getSparseIndex().insert({ 200, 7, 900 });
            writer_manager.commit();
            auto changed_pages = writer_manager.extractChangeLogPages();
            ASSERT_TRUE(flushMeta(writer_meta_space, stream, meta_pair));
            reader_manager.refreshPages(changed_pages);
        }

        auto *reopened_pair = reader_manager.tryGetExisting(0);

        ASSERT_NE(reopened_pair, nullptr);
        auto sparse_item = reopened_pair->getSparseIndex().lookup(200, 7);
        ASSERT_TRUE(!!sparse_item);
        ASSERT_EQ(sparse_item.m_storage_page_num, 900u);
    }
    
    TEST_F( SparsePairTest , testSparsePairCollectsChangeLogOfAddedItems )
    {   
        std::size_t node_size = 16 * 1024;
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
        cut.recordMaxStateNum(1);

        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);

        {
            auto change_log = cut.extractChangeLogPages();
            ASSERT_EQ(change_log, (std::vector<std::uint64_t> { 1, 0 }));
            ASSERT_EQ(cut.getMaxStateNum(), 1u);
        }
        
        std::vector<typename SparseIndex::SI_ItemT> items_2 {
            // page number, state number, physical page number
            { 2, 1, 2 }, { 3, 2, 3 }, { 0, 3, 4 }, { 2, 4, 5 }, { 4, 5, 6 }
        };

        for (auto &item: items_2) {
            sparse_index.insert(item);
        }
        cut.recordMaxStateNum(5);
        
        {
            auto change_log = cut.extractChangeLogPages();
            std::vector<std::uint64_t> expected_data { 2, 3, 0, 2, 4 };
            ASSERT_EQ(change_log, expected_data);
            ASSERT_EQ(cut.getMaxStateNum(), 5u);
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
        
        {
            // create an empty instance
            SparsePair cut(SparsePair::tag_create(), { prefix, allocator});
        }

        int count = 10;
        for (int i = 0; i < count; ++i) {
            SparsePair cut({ prefix, allocator}, AccessType::READ_WRITE, allocator->firstAlloc());
            auto &sparse_index = cut.getSparseIndex();
            for (unsigned int page_num = 0; page_num < 1000; ++page_num) {
                sparse_index.emplace(page_num, i, 999);
            }
            cut.recordMaxStateNum(i);
            
            // simulate change log extraction
            cut.extractChangeLogPages();

            // refresh updates local cached variables with DRAM prefix
            cut.refresh();
            ASSERT_EQ(cut.getMaxStateNum(), i);
        }
    }
    
}
