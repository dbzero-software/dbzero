// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <utils/TestWorkspace.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/memory/BitSpace.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/storage/BDevStorage.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>
#include <algorithm>
#include <thread>

using namespace std;
using namespace db0;
using namespace db0::tests;
    
namespace tests

{
    
    class BDevStorageTest: public testing::Test {
    public:
        static constexpr const char *file_name = "my-test-prefix_1.db0";
        static constexpr const char *copy_file_name = "my-test-prefix-copy.db0";

        virtual void SetUp() override {            
            drop(file_name);
            drop(copy_file_name);
        }

        virtual void TearDown() override {            
            drop(file_name);
            drop(copy_file_name);
        }
    };
    
    // Wrapper class for testing
    class BDevStorageWrapper: public BDevStorage
    {
    public:
        struct DRAMChangeLogRecord
        {
            DRAMChangeLogKind m_kind;
            StateNumType m_state_num;
            std::vector<std::uint64_t> m_page_nums;
        };

        struct DPChangeLogRecord
        {
            StateNumType m_state_num;
            std::vector<std::uint64_t> m_page_nums;
        };

        /**
         * Opens BDevStorage over an existing file
        */
        BDevStorageWrapper(const std::string &file_name, AccessType access_type = AccessType::READ_WRITE,
            LockFlags lock_flags = {}, std::optional<std::size_t> meta_io_step_size = {},
            StorageFlags flags = {}, StorageOptions options = {})
            : BDevStorage(file_name, access_type, lock_flags, meta_io_step_size, flags, options)
        {
        }
        
        PlainSparseIndex &getSparseIndex() {
            return getApplicationSparsePair(0).getSparseIndex();
        }

        PlainSparsePair &getApplicationSparsePair(std::uint64_t page_num) {
            return m_sparse_pair_manager.getOrCreate(getMetaSlotId(page_num));
        }

        const SparsePair &getRootMetaSparsePair() const {
            return m_root_sparse_pair;
        }

        Allocator::SlotId metaSlotId(std::uint64_t page_num) const {
            return getMetaSlotId(page_num);
        }

        std::optional<std::uint64_t> applicationStoragePageNum(
            std::uint64_t logical_page_num, StateNumType state_num)
        {
            auto item = getApplicationSparsePair(logical_page_num)
                .getSparseIndex().lookup(logical_page_num, state_num);
            if (!item) {
                return {};
            }
            std::uint64_t storage_page_num = item.m_storage_page_num;
            return storage_page_num;
        }

        const DRAM_IOStream &getDRAM_IOStream() const {
            return m_dram_io;
        }

        std::vector<DRAMChangeLogRecord> readDRAMChangeLogRecords()
        {
            std::vector<DRAMChangeLogRecord> result;
            DRAM_ChangeLogStreamT::State state;
            m_dram_changelog_io.saveState(state);
            m_dram_changelog_io.setStreamPosHead();
            while (auto change_log = m_dram_changelog_io.readChangeLogChunk()) {
                DRAMChangeLogRecord record { change_log->kind(), change_log->m_state_num, {} };
                for (auto page_num: *change_log) {
                    record.m_page_nums.push_back(page_num);
                }
                result.push_back(std::move(record));
            }
            m_dram_changelog_io.restoreState(state);
            return result;
        }

        std::vector<DPChangeLogRecord> readDPChangeLogRecords()
        {
            std::vector<DPChangeLogRecord> result;
            DP_ChangeLogStreamT::State state;
            m_dp_changelog_io.saveState(state);
            m_dp_changelog_io.setStreamPosHead();
            while (auto change_log = m_dp_changelog_io.readChangeLogChunk()) {
                DPChangeLogRecord record { change_log->m_state_num, {} };
                for (auto page_num: *change_log) {
                    record.m_page_nums.push_back(page_num);
                }
                result.push_back(std::move(record));
            }
            m_dp_changelog_io.restoreState(state);
            return result;
        }

        std::uint32_t getConfigVersion() const {
            return m_config.m_version;
        }

        std::uint64_t appendDescriptorPage(const std::vector<std::byte> &page) {
            return m_descriptor_io.append(page.data());
        }

        void readDescriptorPage(std::uint64_t page_num, std::vector<std::byte> &page) const {
            m_descriptor_io.read(page_num, page.data());
        }

        void dirtyMetaSpaceWithoutStateRegistration() {
            auto address = m_meta_space.alloc(m_config.m_dram_page_size, 1);
            auto lock = m_meta_space.getPrefix().mapRange(
                address.getOffset(), m_config.m_dram_page_size, { AccessOptions::write });
            std::memset(lock.modify(), 0x5a, m_config.m_dram_page_size);
        }

        void recordRootStateForTest(StateNumType state_num) {
            m_root_sparse_pair.recordMaxStateNum(state_num);
        }

        std::optional<std::pair<std::uint64_t, std::uint64_t> > descriptorPageRange() const {
            return m_root_sparse_pair.getDescriptorPageRange();
        }

        std::uint64_t appendDataPage(const std::vector<std::byte> &page) {
            return m_page_io.append(page.data());
        }

        static std::uint64_t physicalOffset(std::uint64_t page_num, std::uint32_t page_size) {
            return CONFIG_BLOCK_SIZE + page_num * page_size;
        }

        void readMetered(std::uint64_t address, std::uint64_t state_num, std::size_t size, void *buffer,
            unsigned int &chain_len) const
        {
            _read(address, state_num, size, buffer, { AccessOptions::read }, &chain_len);
        }
    };

    TEST_F( BDevStorageTest , testCanCreateEmptyDB0FileWithDefaultConfiguration )
    {         
        BDevStorage::create(file_name);
        ASSERT_TRUE(file_exists(file_name));
    }

    TEST_F( BDevStorageTest , testApplicationSparsePairIsHostedInMetaSpace )
    {
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        std::vector<char> page(page_size, 0x41);

        {
            BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);
            cut.write(0, 1, page.size(), page.data());
            ASSERT_TRUE(cut.flush());

            auto &app_pair = cut.getApplicationSparsePair(0);
            ASSERT_TRUE(app_pair.getSparseIndex().lookup(0, 1));
            ASSERT_GT(cut.getRootMetaSparsePair().size(), 0u);
            cut.close();
        }

        {
            BDevStorageWrapper reopened(file_name, AccessType::READ_ONLY);
            std::vector<char> read_buffer(page_size);
            reopened.read(0, 1, read_buffer.size(), read_buffer.data(), { AccessOptions::read });
            ASSERT_TRUE(equal(page, read_buffer));
            ASSERT_TRUE(reopened.getApplicationSparsePair(0).getSparseIndex().lookup(0, 1));
            reopened.close();
        }
    }

    TEST_F( BDevStorageTest , testApplicationSparsePairBucketingUsesConfiguredFunction )
    {
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);

        StorageOptions options;
        options.m_storage_slab_bucketing = [page_size](std::uint64_t address) {
            return address < page_size * 10 ? 5u : 9u;
        };

        BDevStorageWrapper cut(file_name, AccessType::READ_WRITE, {}, {}, {}, options);
        std::vector<char> low_page(page_size, 0x15);
        std::vector<char> high_page(page_size, 0x19);
        cut.write(0, 1, low_page.size(), low_page.data());
        cut.write(20 * page_size, 1, high_page.size(), high_page.data());

        auto &low_pair = cut.getApplicationSparsePair(0);
        auto &high_pair = cut.getApplicationSparsePair(20);
        auto low_slot = MS_MetaPrefix::slotIdFromPageNum(
            low_pair.getSparseIndex().getIndexAddress().getOffset() / cut.getDescriptorPageSize());
        auto high_slot = MS_MetaPrefix::slotIdFromPageNum(
            high_pair.getSparseIndex().getIndexAddress().getOffset() / cut.getDescriptorPageSize());

        ASSERT_EQ(cut.metaSlotId(0), 5u);
        ASSERT_EQ(cut.metaSlotId(20), 9u);
        ASSERT_EQ(low_slot, 5u);
        ASSERT_EQ(high_slot, 9u);
        ASSERT_NE(&low_pair, &high_pair);
        cut.close();
    }

    TEST_F( BDevStorageTest , testSparsePairQueryUsesBucketSpanOnlyForMultiPageRanges )
    {
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);

        unsigned int single_page_mapping_calls = 0;
        unsigned int bucket_mapping_calls = 0;

        StorageOptions options;
        options.m_storage_slab_bucketing = [&](std::uint64_t) {
            ++single_page_mapping_calls;
            return 0u;
        };
        options.m_storage_slab_bucket = [&](std::uint64_t) {
            ++bucket_mapping_calls;
            return StorageOptions::StorageSlabBucket { 0u, 0u, 1024u };
        };

        BDevStorageWrapper cut(file_name, AccessType::READ_WRITE, {}, {}, {}, options);
        std::vector<char> write_buffer(2 * page_size, 0x32);
        single_page_mapping_calls = 0;
        bucket_mapping_calls = 0;
        cut.write(0, 1, write_buffer.size(), write_buffer.data());
        ASSERT_EQ(single_page_mapping_calls, 0u);
        ASSERT_EQ(bucket_mapping_calls, 1u);

        single_page_mapping_calls = 0;
        bucket_mapping_calls = 0;
        std::vector<char> single_page_read(page_size);
        cut.read(0, 1, single_page_read.size(), single_page_read.data(), { AccessOptions::read });
        ASSERT_EQ(single_page_mapping_calls, 1u);
        ASSERT_EQ(bucket_mapping_calls, 0u);

        single_page_mapping_calls = 0;
        bucket_mapping_calls = 0;
        std::vector<char> multi_page_read(2 * page_size);
        cut.read(0, 1, multi_page_read.size(), multi_page_read.data(), { AccessOptions::read });
        ASSERT_EQ(single_page_mapping_calls, 0u);
        ASSERT_EQ(bucket_mapping_calls, 1u);
        ASSERT_TRUE(equal(write_buffer, multi_page_read));

        cut.close();
    }

    TEST_F( BDevStorageTest , testSparsePairQueryRefreshesAtBucketBoundaryWithoutSkippingFirstPage )
    {
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);

        unsigned int single_page_mapping_calls = 0;
        unsigned int bucket_mapping_calls = 0;

        StorageOptions options;
        options.m_storage_slab_bucketing = [&](std::uint64_t address) {
            ++single_page_mapping_calls;
            return static_cast<std::uint32_t>(address / page_size);
        };
        options.m_storage_slab_bucket = [&](std::uint64_t address) {
            ++bucket_mapping_calls;
            auto page_num = address / page_size;
            return StorageOptions::StorageSlabBucket {
                static_cast<std::uint32_t>(page_num), page_num, page_num + 1
            };
        };

        BDevStorageWrapper cut(file_name, AccessType::READ_WRITE, {}, {}, {}, options);
        std::vector<char> write_buffer(2 * page_size);
        std::fill(write_buffer.begin(), write_buffer.begin() + page_size, 0x31);
        std::fill(write_buffer.begin() + page_size, write_buffer.end(), 0x42);

        cut.write(0, 1, write_buffer.size(), write_buffer.data());
        ASSERT_EQ(single_page_mapping_calls, 0u);
        ASSERT_EQ(bucket_mapping_calls, 2u);

        single_page_mapping_calls = 0;
        bucket_mapping_calls = 0;
        std::vector<char> read_buffer(2 * page_size);
        cut.read(0, 1, read_buffer.size(), read_buffer.data(), { AccessOptions::read });
        ASSERT_EQ(single_page_mapping_calls, 0u);
        ASSERT_EQ(bucket_mapping_calls, 2u);
        ASSERT_TRUE(equal(write_buffer, read_buffer));

        cut.close();
    }

    TEST_F( BDevStorageTest , testDescriptorIOUsesSeparatePageSizeAndDoesNotCollideWithPageIO )
    {
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);

        std::uint64_t descriptor_page_num = 0;
        std::uint64_t data_page_num = 0;
        std::vector<std::byte> descriptor_page(16u << 10, std::byte{0x55});
        std::vector<std::byte> data_page(page_size, std::byte{0x2a});
        std::vector<char> state_page(page_size, 0x15);

        {
            BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);
            ASSERT_EQ(2u, cut.getConfigVersion());
            ASSERT_EQ(page_size, cut.getPageSize());
            ASSERT_EQ(16u << 10, cut.getDescriptorPageSize());

            cut.write(0, 1, state_page.size(), state_page.data());
            descriptor_page_num = cut.appendDescriptorPage(descriptor_page);
            data_page_num = cut.appendDataPage(data_page);
            cut.close();
        }

        auto descriptor_begin = BDevStorageWrapper::physicalOffset(descriptor_page_num, 16u << 10);
        auto descriptor_end = descriptor_begin + descriptor_page.size();
        auto data_begin = BDevStorageWrapper::physicalOffset(data_page_num, page_size);
        auto data_end = data_begin + data_page.size();
        ASSERT_TRUE(descriptor_end <= data_begin || data_end <= descriptor_begin);

        {
            BDevStorageWrapper cut(file_name, AccessType::READ_ONLY);
            std::vector<std::byte> descriptor_read(descriptor_page.size());
            cut.readDescriptorPage(descriptor_page_num, descriptor_read);
            ASSERT_EQ(descriptor_page, descriptor_read);
            cut.close();
        }
    }

    TEST_F( BDevStorageTest , testDescriptorIOCursorIsRestoredFromRootMetadata )
    {
        BDevStorage::create(file_name);

        std::uint64_t first_page_num = 0;
        std::uint64_t second_page_num = 0;
        std::vector<std::byte> first_page(16u << 10, std::byte{0x11});
        std::vector<std::byte> second_page(16u << 10, std::byte{0x22});
        std::size_t page_size = 4096;
        std::vector<char> data_page(page_size, 0x33);

        {
            BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);
            cut.write(0, 1, data_page.size(), data_page.data());
            first_page_num = cut.appendDescriptorPage(first_page);
            cut.close();
        }

        {
            BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);
            auto descriptor_page_range = cut.descriptorPageRange();
            ASSERT_TRUE(descriptor_page_range);
            ASSERT_EQ(first_page_num, descriptor_page_range->first);
            ASSERT_GE(descriptor_page_range->second, first_page_num + 1);
            cut.write(page_size, 2, data_page.size(), data_page.data());
            second_page_num = cut.appendDescriptorPage(second_page);
            ASSERT_GT(second_page_num, first_page_num);
            cut.close();
        }

        {
            BDevStorageWrapper cut(file_name, AccessType::READ_ONLY);
            std::vector<std::byte> first_read(first_page.size());
            std::vector<std::byte> second_read(second_page.size());
            cut.readDescriptorPage(first_page_num, first_read);
            cut.readDescriptorPage(second_page_num, second_read);
            ASSERT_EQ(first_page, first_read);
            ASSERT_EQ(second_page, second_read);
            auto descriptor_page_range = cut.descriptorPageRange();
            ASSERT_TRUE(descriptor_page_range);
            ASSERT_EQ(first_page_num, descriptor_page_range->first);
            ASSERT_GE(descriptor_page_range->second, second_page_num + 1);
            cut.close();
        }
    }

    TEST_F( BDevStorageTest , testCopyToCopiesDescriptorIOExactly )
    {
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size, (16u << 10) - 256, 4u << 20);

        std::vector<char> data_page(page_size, 0x11);
        std::vector<char> second_data_page(page_size, 0x22);
        {
            BDevStorageWrapper src(file_name, AccessType::READ_WRITE);
            src.write(0, 1, data_page.size(), data_page.data());
            ASSERT_TRUE(src.flush());
            src.write(page_size, 2, second_data_page.size(), second_data_page.data());
            ASSERT_TRUE(src.flush());
            src.close();
        }

        std::pair<std::uint64_t, std::uint64_t> descriptor_page_range;
        {
            BDevStorageWrapper src_before_copy(file_name, AccessType::READ_ONLY);
            auto maybe_descriptor_page_range = src_before_copy.descriptorPageRange();
            ASSERT_TRUE(maybe_descriptor_page_range);
            descriptor_page_range = *maybe_descriptor_page_range;
            ASSERT_LT(descriptor_page_range.first, descriptor_page_range.second);
            src_before_copy.close();
        }

        {
            BDevStorageWrapper src(file_name, AccessType::READ_ONLY);
            BDevStorage::create(copy_file_name, page_size, (16u << 10) - 256, 4u << 20);
            BDevStorageWrapper out(copy_file_name, AccessType::READ_WRITE);
            src.copyTo(out);
            out.close();
            src.close();
        }

        BDevStorageWrapper src(file_name, AccessType::READ_ONLY);
        BDevStorageWrapper out(copy_file_name, AccessType::READ_ONLY);
        ASSERT_TRUE(src.descriptorPageRange());
        ASSERT_TRUE(out.descriptorPageRange());
        ASSERT_EQ(src.descriptorPageRange(), out.descriptorPageRange());
        ASSERT_EQ(descriptor_page_range, *out.descriptorPageRange());

        for (auto descriptor_page_num = descriptor_page_range.first;
             descriptor_page_num < descriptor_page_range.second; ++descriptor_page_num) {
            std::vector<std::byte> src_descriptor_read(src.getDescriptorPageSize());
            std::vector<std::byte> out_descriptor_read(out.getDescriptorPageSize());
            src.readDescriptorPage(descriptor_page_num, src_descriptor_read);
            out.readDescriptorPage(descriptor_page_num, out_descriptor_read);
            ASSERT_EQ(src_descriptor_read, out_descriptor_read);
        }

        std::vector<char> data_read(page_size);
        out.read(0, 1, data_read.size(), data_read.data(), { AccessOptions::read });
        ASSERT_TRUE(equal(data_page, data_read));
        out.read(page_size, 2, data_read.size(), data_read.data(), { AccessOptions::read });
        ASSERT_TRUE(equal(second_data_page, data_read));
        out.close();
        src.close();
    }

    TEST_F( BDevStorageTest , testCanWriteThenReadFullPagesFromOneState )
    { 
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name);
        auto page_size = cut.getPageSize();
        // a valid state number must be > 0
        auto state_num = 1;
        std::unordered_map<std::uint64_t, std::vector<char>> pages;
        for (int i = 0; i < 100; ++i) {
            auto page_num = rand() % 10000;
            if (pages.find(page_num) != pages.end()) {
                continue;
            }
            auto &page = pages.insert({page_num, randomPage(page_size)}).first->second;
            cut.write(page_num * page_size, state_num, page.size(), page.data());
        }

        // read pages & validate contents
        for (auto &page: pages) {
            std::vector<char> read_buffer(page_size);
            cut.read(page.first * page_size, state_num, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(page.second, read_buffer));
        }
        cut.close();
    }

    TEST_F( BDevStorageTest , testCanReadPagesFromDifferentStates )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name);
        std::deque<std::vector<char> > pages;
        for (int i = 0;i < 10;++i) {
            auto state_num = 1 + i * 5;
            pages.push_back(randomPage(cut.getPageSize()));
            // write page under state "i * 5"
            cut.write(0, state_num, pages.back().size(), pages.back().data());
        }
        // pairs of query / expected states
        std::vector<std::pair<int, int> > states = {
            {0 + 1, 0 + 1}, {3 + 1, 0 + 1}, {11 + 1, 10 + 1}, {33 + 1, 30 + 1}, {34 + 1, 30 + 1}, 
            {51 + 1, 45 + 1}, {99 + 1, 45 + 1}, {12 + 1, 10 + 1}
        };
        for (auto &p: states) {
            std::vector<char> read_buffer(cut.getPageSize());
            cut.read(0, p.first, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(pages[p.second / 5], read_buffer));
        }
        cut.close();
    }
    
    TEST_F( BDevStorageTest , testSparseIndexIsSerializedOnClose )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        std::deque<std::vector<char> > pages;
        {
            BDevStorage cut(file_name);
            for (int i = 0;i < 10;++i) {
                pages.push_back(randomPage(cut.getPageSize()));
                auto state_num = 1 + i * 5;
                // write page under state "i * 5"
                cut.write(0, state_num, pages.back().size(), pages.back().data());
            }
            cut.close();
        }
        // open storage again
        BDevStorage cut(file_name);
        // pairs of query / expected states
        std::vector<std::pair<int, int> > states = {
            {0 + 1, 0 + 1}, {3 + 1, 0 + 1}, {11 + 1, 10 + 1}, {33 + 1, 30 + 1}, {34 + 1, 30 + 1}, 
            {51 + 1, 45 + 1}, {99 + 1, 45 + 1}, {12 + 1, 10 + 1}
        };
        for (auto &p: states) {
            std::vector<char> read_buffer(cut.getPageSize());
            cut.read(0, p.first, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(pages[p.second / 5], read_buffer));
        }
        cut.close();
    }

    TEST_F( BDevStorageTest , testSparsePairManagerChangeLogIsStoredInDRAMChangeLog )
    {
        BDevStorage::create(file_name);
        BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);
        auto page = randomPage(cut.getPageSize());

        cut.write(0, 1, page.size(), page.data());
        ASSERT_TRUE(cut.flush());

        bool found_sparse_pair_manager_record = false;
        for (const auto &record: cut.readDRAMChangeLogRecords()) {
            if (record.m_kind == DRAMChangeLogKind::SPARSE_PAIR_MANAGER && record.m_state_num == 1) {
                found_sparse_pair_manager_record = true;
                ASSERT_EQ(record.m_page_nums, (std::vector<std::uint64_t> { 0 }));
            }
        }
        ASSERT_TRUE(found_sparse_pair_manager_record);

        for (const auto &record: cut.readDPChangeLogRecords()) {
            ASSERT_NE(record.m_state_num, 1u);
        }
        cut.close();
    }

    TEST_F( BDevStorageTest , testBDevStorageThrowsIfReadingFromUninitializedSpace )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name, AccessType::READ_ONLY);
        std::vector<char> buffer(cut.getPageSize());
        ASSERT_ANY_THROW(cut.read(0, 1, cut.getPageSize(), buffer.data(), { AccessOptions::read }));
    }
    
    TEST_F( BDevStorageTest , testBDevStorageZeroInitializeNewPagesIfAccessedForWriteOnly )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name);
        std::vector<char> buffer(cut.getPageSize());
        std::vector<char> zero_buffer(cut.getPageSize());
        memset(zero_buffer.data(), 0, zero_buffer.size());
        
        cut.read(0, 1, cut.getPageSize(), buffer.data(), { AccessOptions::write });
        ASSERT_TRUE(equal(zero_buffer, buffer));
        cut.close();
    }
    
    TEST_F( BDevStorageTest , testCanFindMutation )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name);
        auto page_size = cut.getPageSize();
        std::deque<std::vector<char> > pages;
        for (int i = 0;i < 10;++i) {            
            pages.push_back(randomPage(page_size));
            // a valid state num must be > 0
            auto state_num = 1 + i * 5;
            // write page under state "i * 5", address = page num * page_size
            cut.write(i * page_size, state_num, page_size, pages.back().data());
        }
        ASSERT_EQ(cut.findMutation(0, 1 + 3), 1);
        // unable to read page #1 (not yet available in state = 1)
        StateNumType mutation_id;
        ASSERT_FALSE(cut.tryFindMutation(1, 1, mutation_id));
        cut.close();
    }

    TEST_F( BDevStorageTest , testSparseIndexIsProperlySerializedAfterUpdates )
    {
        srand(9142424u);
        BDevStorage::create(file_name);
        std::deque<std::vector<char> > pages_v1;
        {
            BDevStorage cut(file_name);
            for (int i = 0;i < 10;++i) {
                pages_v1.push_back(randomPage(cut.getPageSize()));
                // write pages under state "1"
                cut.write(i * cut.getPageSize(), 1, pages_v1.back().size(), pages_v1.back().data());
            }
            cut.close();
        }
        
        std::deque<std::vector<char> > pages_v2;
        {
            BDevStorage cut(file_name);
            for (int i = 0;i < 10;++i) {
                pages_v2.push_back(randomPage(cut.getPageSize()));
                // write pages under state "2"
                cut.write(i * cut.getPageSize(), 2, pages_v2.back().size(), pages_v2.back().data());
            }
            cut.close();
        }

        // open storage and try retrieving both versions
        BDevStorage cut(file_name);
        for (int i = 0;i < 10;++i) {
            std::vector<char> read_buffer(cut.getPageSize());
            cut.read(i * cut.getPageSize(), 1, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(pages_v1[i], read_buffer));
            cut.read(i * cut.getPageSize(), 2, read_buffer.size(), read_buffer.data());
            ASSERT_TRUE(equal(pages_v2[i], read_buffer));
        }
        cut.close();
    }

    TEST_F( BDevStorageTest , testReopenedWriterAppendsUpdatedPagesAfterExistingData )
    {
        BDevStorage::create(file_name);
        std::size_t page_size = 0;

        {
            BDevStorage cut(file_name);
            page_size = cut.getPageSize();
            for (int i = 0; i < 3; ++i) {
                std::vector<char> page(page_size, static_cast<char>('a' + i));
                cut.write(i * page_size, 1, page.size(), page.data());
            }
            cut.close();
        }

        {
            BDevStorage cut(file_name);
            for (int i = 0; i < 3; ++i) {
                std::vector<char> page(page_size, static_cast<char>('A' + i));
                cut.write(i * page_size, 2, page.size(), page.data());
            }
            cut.close();
        }

        BDevStorageWrapper cut(file_name);
        for (int i = 0; i < 3; ++i) {
            std::vector<char> read_buffer(page_size);
            cut.read(i * page_size, 1, read_buffer.size(), read_buffer.data());
            ASSERT_EQ(read_buffer[0], static_cast<char>('a' + i));

            cut.read(i * page_size, 2, read_buffer.size(), read_buffer.data());
            ASSERT_EQ(read_buffer[0], static_cast<char>('A' + i))
                << "logical_page=" << i
                << " storage_page="
                << cut.applicationStoragePageNum(static_cast<std::uint64_t>(i), 2).value_or(0);
        }
        cut.close();
    }

    TEST_F( BDevStorageTest , testStateWiseWriteThenRead )
    {   
        // In this test scenario we simply perform a sequence of writes
        // and then read and validate contents
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        // Write operations to be performed, each operation will be performed within a dedicated state        
        // (address, span, character)
        std::vector<std::tuple<std::uint64_t, std::size_t, char> > write_ops = {
            { 1, 1, 'a'}, { 2, 1, 'b'}, { 3, 1, 'c'}, { 4, 3, 'a'},
            { 17, 4, 'c'}, { 1, 3, 'a'}, { 7, 3, 'z'}, { 2, 8, 'x'}
        };

        // Writer, eaach write performed under a different state number
        {
            BDevStorage cut(file_name, AccessType::READ_WRITE);
            StateNumType state_num = 1;
            for (auto &op: write_ops) {
                std::vector<char> data(std::get<1>(op) * page_size, std::get<2>(op));
                cut.write(std::get<0>(op) * page_size, state_num, data.size(), data.data());
                // flush after each write for additional validation
                cut.flush();
                ++state_num;
            }
            cut.close();
        }
        
        // Reader, validate contents
        {
            BDevStorage cut(file_name, AccessType::READ_ONLY);
            StateNumType state_num = 1;
            for (auto &op: write_ops) {
                std::vector<char> buffer(std::get<1>(op) * page_size);
                cut.read(std::get<0>(op) * page_size, state_num, buffer.size(), buffer.data(), { AccessOptions::read });
                // validate contents
                for (std::size_t i = 0;i < buffer.size();++i) {
                    ASSERT_EQ(buffer[i], std::get<2>(op));
                }                
                ++state_num;
            }
            cut.close();
        }
    }

    TEST_F( BDevStorageTest , testReadAfterFlushButWithoutClose )
    {   
        // In this test scenario we perform sequence of write/flush
        // and try reading before closing the output stream        
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        // Write operations to be performed, each operation will be performed within a dedicated state        
        // (address, span, character)
        std::vector<std::tuple<std::uint64_t, std::size_t, char> > write_ops = {
            { 1, 1, 'a'}, { 2, 1, 'b'}, { 3, 1, 'c'}, { 4, 3, 'a'},
            { 17, 4, 'c'}, { 1, 3, 'a'}, { 7, 3, 'z'}, { 2, 8, 'x'}
        };

        // Writer, eaach write performed under a different state number    
        BDevStorage cut(file_name, AccessType::READ_WRITE);
        StateNumType state_num = 1;
        for (auto &op: write_ops) {
            std::vector<char> data(std::get<1>(op) * page_size, std::get<2>(op));
            cut.write(std::get<0>(op) * page_size, state_num, data.size(), data.data());
            // flush after each write for additional validation
            cut.flush();

            // Attempt reading before close
            {
                BDevStorage reader(file_name, AccessType::READ_ONLY);
                std::vector<char> buffer(std::get<1>(op) * page_size);
                reader.read(std::get<0>(op) * page_size, state_num, buffer.size(), buffer.data(), { AccessOptions::read });
                // validate contents
                for (std::size_t i = 0;i < buffer.size();++i) {
                    ASSERT_EQ(buffer[i], std::get<2>(op));
                }
                reader.close();
            }

            ++state_num;
        }
        cut.close();        
    }

    TEST_F( BDevStorageTest , testConcurrentStorageWriterAndReaderWithClose )
    {
        // This is to test the scenario when file is flushed and the modifications
        // should be accessible to a newly opened read-only instance, no refresh called     
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        
        // Write operations to be performed, each operation will be performed within a dedicated state        
        // (address, span, character)
        std::vector<std::tuple<std::uint64_t, std::size_t, char> > write_ops = {
            { 1, 1, 'a'}, { 2, 1, 'b'}, { 3, 1, 'c'}, { 4, 3, 'a'},
            { 17, 4, 'c'}, { 1, 3, 'a'}, { 7, 3, 'z'}, { 2, 8, 'x'}
        };
        
        // Start reader from a separate thread
        std::thread reader([&]()
        {
            StateNumType state_num = 1;
            for (auto &op: write_ops) {
                bool success = false;
                while (!success) {
                    BDevStorage storage(file_name, AccessType::READ_ONLY);
                    // only attempt reading when the state is available
                    if (storage.getMaxStateNum() >= state_num) {
                        std::vector<char> buffer(std::get<1>(op) * page_size);
                        storage.read(std::get<0>(op) * page_size, state_num, buffer.size(), buffer.data(), { AccessOptions::read });
                        // validate contents
                        for (std::size_t i = 0;i < buffer.size();++i) {
                            ASSERT_EQ(buffer[i], std::get<2>(op));
                        }
                        success = true;
                    }
                    storage.close();
                    // sleep before making another attempt                    
                    if (!success) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    }
                }
                ++state_num;
            }
        });
        
        BDevStorage cut(file_name, AccessType::READ_WRITE);
        StateNumType state_num = 1;
        for (auto &op: write_ops) {
            std::vector<char> data(std::get<1>(op) * page_size, std::get<2>(op));
            cut.write(std::get<0>(op) * page_size, state_num, data.size(), data.data());
            // flush data after each write
            cut.flush();
            ++state_num;
            // sleep 25ms
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        cut.close();
        reader.join();
    }
    
    TEST_F( BDevStorageTest , testConcurrentWriterAndReaderUsingRefresh )
    {
        // In this test case the reader is not closing the storage but using 'refresh' to 
        // sync to the latest changes
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        // Write operations to be performed, each operation will be performed within a dedicated state        
        // (address, span, character)
        std::vector<std::tuple<std::uint64_t, std::size_t, char> > write_ops = {
            { 1, 1, 'a'}, { 2, 1, 'b'}, { 3, 1, 'c'}, { 4, 3, 'a'},
            { 17, 4, 'c'}, { 1, 3, 'a'}, { 7, 3, 'z'}, { 2, 8, 'x'}
        };
        
        // Start reader from a separate thread
        std::thread reader([&]()
        {
            StateNumType state_num = 1;
            BDevStorage storage(file_name, AccessType::READ_ONLY);
            for (auto &op: write_ops) {
                bool success = false;
                while (!success) {
                    // refresh before making an attempt
                    storage.refresh();
                    // only attempt reading when the state is available
                    if (storage.getMaxStateNum() >= state_num) {
                        std::vector<char> buffer(std::get<1>(op) * page_size);
                        storage.read(std::get<0>(op) * page_size, state_num, buffer.size(), buffer.data(), { AccessOptions::read });
                        // validate contents
                        for (std::size_t i = 0;i < buffer.size();++i) {
                            ASSERT_EQ(buffer[i], std::get<2>(op));
                        }
                        success = true;
                    }

                    // sleep before making another attempt
                    if (!success) {                        
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    }
                }
                ++state_num;
            }
            storage.close();
        });
        
        BDevStorage cut(file_name, AccessType::READ_WRITE);
        StateNumType state_num = 1;
        for (auto &op: write_ops) {
            std::vector<char> data(std::get<1>(op) * page_size, std::get<2>(op));
            cut.write(std::get<0>(op) * page_size, state_num, data.size(), data.data());
            // flush data after each write
            cut.flush();
            ++state_num;
            // sleep 25ms
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }

        cut.close();
        reader.join();
    }

    TEST_F( BDevStorageTest , testReaderRefreshSeesRepeatedWritesInSameSparsePairSlot )
    {
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);

        BDevStorage writer(file_name, AccessType::READ_WRITE);
        BDevStorage reader(file_name, AccessType::READ_ONLY);

        std::vector<char> first(page_size, 'a');
        writer.write(page_size, 1, first.size(), first.data());
        writer.flush();

        reader.refresh();
        ASSERT_GE(reader.getMaxStateNum(), 1u);
        std::vector<char> buffer(page_size);
        reader.read(page_size, 1, buffer.size(), buffer.data(), { AccessOptions::read });
        ASSERT_TRUE(equal(first, buffer));

        std::vector<char> second(page_size, 'b');
        writer.write(2 * page_size, 2, second.size(), second.data());
        writer.flush();

        reader.refresh();
        ASSERT_GE(reader.getMaxStateNum(), 2u);
        reader.read(2 * page_size, 2, buffer.size(), buffer.data(), { AccessOptions::read });
        ASSERT_TRUE(equal(second, buffer));
        writer.close();
        reader.close();
    }

    TEST_F( BDevStorageTest , testNoLoadReaderCanRefreshAfterWriterCommit )
    {
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);

        BDevStorage reader(file_name, AccessType::READ_ONLY, {}, {}, { StorageFlagOption::NO_LOAD });

        std::vector<char> data(page_size, 'r');
        {
            BDevStorage writer(file_name, AccessType::READ_WRITE);
            writer.write(0, 1, data.size(), data.data());
            writer.flush();
            writer.close();
        }

        ASSERT_NO_THROW(reader.refresh());
        ASSERT_EQ(reader.getMaxStateNum(), 1u);

        std::vector<char> buffer(page_size);
        reader.read(0, 1, buffer.size(), buffer.data(), { AccessOptions::read });
        ASSERT_TRUE(equal(data, buffer));
        reader.close();
    }

    TEST_F( BDevStorageTest , testFlushRejectsDirtyMetadataWithoutRegisteredStateHighWatermark )
    {
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);

        BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);
        cut.dirtyMetaSpaceWithoutStateRegistration();

        ASSERT_THROW(cut.flush(), db0::InternalException);
        cut.recordRootStateForTest(1);
        cut.close();
    }
    
    TEST_F( BDevStorageTest , testSparseIndexDurability )
    {   
        // In this test scenario we perform sequence of write/flush
        // and try reading before closing the output stream        
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);
        auto count = 10;
        std::optional<int> last_state_num;
        for (int i = 0; i < count; ++i) {
            BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);
            auto state_num = static_cast<StateNumType>(i + 1);
                        
            if (last_state_num) {
                ASSERT_EQ(cut.getMaxStateNum(), *last_state_num);
            }
            std::vector<char> data(page_size);
            for (unsigned int page_num = 0; page_num < 1000; ++page_num) {
                cut.write(page_num * page_size, state_num, data.size(), data.data());

                cut.getSparseIndex().refresh();
                ASSERT_EQ(cut.getRootMetaSparsePair().getMaxStateNum(), state_num);
            }
            
            cut.getSparseIndex().refresh();
            ASSERT_EQ(cut.getRootMetaSparsePair().getMaxStateNum(), state_num);
            cut.close();
            last_state_num = state_num;
        }
    }
    
    TEST_F( BDevStorageTest , testDiffsChainIsLimited )
    {   
        // in this test we perform as sequence of writes usind the diff-storage
        // the expected outcome is that the diff-chain is limited to the specified length
        std::size_t page_size = 4096;
        BDevStorage::create(file_name, page_size);        
        std::optional<int> last_state_num;
        BDevStorageWrapper cut(file_name, AccessType::READ_WRITE);

        std::vector<std::byte> dp_0(page_size, std::byte{0});
        std::vector<std::byte> dp_1(page_size, std::byte{0});
        unsigned int max_len = 16;
        for (int i = 1; i < 100; ++i) {
            std::vector<std::uint16_t> diffs;
            dp_1[150] = (std::byte)(i + 1);
            ASSERT_TRUE(db0::getDiffs(dp_0.data(), dp_1.data(), dp_1.size(), diffs));            
            if (!cut.tryWriteDiffs(0, i + 1, page_size, dp_1.data(), diffs, max_len)) {
                cut.write(0, i + 1, page_size, dp_1.data());
            }
            cut.flush();
            std::memcpy(dp_0.data(), dp_1.data(), dp_1.size());
        }
        
        // now, reading the data from past transactions verify that then chain length is limited
        unsigned int last_chain_len = 0;
        for (int i = 1; i < 100; ++i) {
            std::vector<char> buffer(page_size);
            unsigned int chain_len = 0;
            cut.readMetered(0, i + 1, buffer.size(), buffer.data(), chain_len);
            ASSERT_EQ(buffer[150], i + 1);
            ASSERT_TRUE(chain_len <= max_len);
            ASSERT_TRUE(chain_len >= last_chain_len || chain_len <= 1);
            last_chain_len = chain_len;
        }
        cut.close();
    }
    
    TEST_F( BDevStorageTest , testBDevStorageFetchChangeLogs )
    {
        using DP_ChangeLogT = db0::BaseStorage::DP_ChangeLogT;

        srand(9142424u);
        BDevStorage::create(file_name);
        BDevStorage cut(file_name, AccessType::READ_WRITE, {}, 16);

        std::vector<std::vector<std::uint64_t> > updates {
            { 0, 1, 2, 3 },
            { 14, 13, 10, 11, 12 },
            { 21, 20 },
            { 35, 30, 31, 32, 33, 34 },
            { 43, 40, 44, 41, 42 },
            { 50, 51, 52, 53 },
            { 60, 61, 62, 63, 64 },
            { 73, 72, 70, 71 },
            { 80, 81, 82, 83 },
            { 91, 90 }
        };

        StateNumType state_num = 1;
        for (auto &page_nums: updates) {
            std::vector<char> page(cut.getPageSize());
            for (auto &page_num: page_nums) {
                std::memset(page.data(), page_num, page.size());
                cut.write(page_num * cut.getPageSize(), state_num, page.size(), page.data());
            }
            cut.flush();
            ++state_num;
        }

        std::vector<std::pair<StateNumType, std::optional<StateNumType> > > state_ranges {
            { 9, std::nullopt },
            { 4, 10 },
            { 8, 12 },
            { 3, 7 }
        };

        std::vector<std::vector<StateNumType> > expected_state_nums {
            { 9, 10 },
            { 4, 5, 6, 7, 8, 9 },
            { 8, 9, 10 },
            { 3, 4, 5, 6 }
        };
        
        unsigned int range_id = 0;
        for (auto range: state_ranges) {
            // collect and validate change-logs
            std::vector<StateNumType> state_nums;
            cut.fetchDP_ChangeLogs(range.first, range.second, [&](const DP_ChangeLogT &cl) {
                state_nums.push_back(cl.m_state_num);
                std::vector<std::uint64_t> page_nums;
                for (auto page_num: cl) {
                    page_nums.push_back(page_num);
                }
                auto sorted_updates = updates[cl.m_state_num - 1];
                std::sort(sorted_updates.begin(), sorted_updates.end());
                ASSERT_EQ(page_nums, sorted_updates);
            });
            
            ASSERT_EQ(state_nums, expected_state_nums[range_id]);
            ++range_id;
        }
        
        cut.close();
    }

}
