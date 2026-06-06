// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <algorithm>
#include <cstring>
#include <map>
#include <random>
#include <utils/utils.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/MetaPrefix.hpp>
#include <dbzero/core/dram/MetaSpace.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
#include <dbzero/core/storage/SparseIndexQuery.hpp>
#include <dbzero/core/storage/SparsePair.hpp>

using namespace db0;
using namespace db0::tests;

namespace tests
{

    class MetaSpaceTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "my-test-metaspace.io";
        static constexpr std::size_t page_size = 4096;

        void SetUp() override {
            drop(file_name);
        }

        void TearDown() override {
            drop(file_name);
        }

        static Diff_IO createIO(CFile &file)
        {
            return createIO(file, page_size);
        }

        static Diff_IO createIO(CFile &file, std::size_t page_size)
        {
            auto tail_function = [&file]() -> std::uint64_t {
                return file.size();
            };
            return Diff_IO(0, file, page_size, page_size * 16, page_size, 0, 1, tail_function, 0, 4);
        }

        static DRAM_Pair createMappingPair()
        {
            return createMappingPair(page_size);
        }

        static DRAM_Pair createMappingPair(std::size_t page_size)
        {
            return {
                std::make_shared<DRAM_Prefix>(page_size),
                std::make_shared<DRAM_Allocator>(page_size)
            };
        }

        static void fillPage(Memspace &memspace, Address address, unsigned char value)
        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            std::memset(lock.modify(), value, page_size);
        }

        static std::vector<unsigned char> readPage(Memspace &memspace, Address address)
        {
            std::vector<unsigned char> result(page_size);
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::read });
            std::memcpy(result.data(), static_cast<void *>(lock), page_size);
            return result;
        }

        static bool flushMeta(Memspace &memspace, Diff_IO &io)
        {
            return flush(dynamic_cast<MetaPrefix &>(memspace.getPrefix()), io);
        }

        static bool compactMeta(Memspace &memspace, Diff_IO &io)
        {
            return compact(dynamic_cast<MetaPrefix &>(memspace.getPrefix()), io);
        }

        static DRAM_Pair createPairFromMetaSpace(Memspace &memspace)
        {
            auto prefix = std::dynamic_pointer_cast<DRAM_Prefix>(memspace.getPrefixPtr());
            auto meta_prefix = std::dynamic_pointer_cast<MetaPrefix>(prefix);
            auto allocator = std::make_shared<DRAM_Allocator>(
                [meta_prefix](DRAM_Allocator::AddressSinkFunction sink) {
                    meta_prefix->forAllocatedAddresses([&](std::size_t address) {
                        if (address != 0) {
                            sink(address);
                        }
                    });
                },
                memspace.getPageSize()
            );
            return { prefix, allocator };
        }

        static std::optional<std::uint64_t> findDiffStoragePage(const DI_Item &item, std::uint32_t state_num)
        {
            if (item.m_state_num == state_num) {
                return item.m_storage_page_num;
            }

            std::uint32_t next_state_num = 0;
            std::uint64_t next_storage_page_num = 0;
            auto it = item.beginDiff();
            while (it.next(next_state_num, next_storage_page_num)) {
                if (next_state_num == state_num) {
                    return next_storage_page_num;
                }
            }
            return std::nullopt;
        }

        static std::vector<unsigned char> readStoragePage(Diff_IO &io, std::uint64_t storage_page_num)
        {
            std::vector<unsigned char> result(page_size);
            io.read(storage_page_num, result.data());
            return result;
        }

        static void patchExpectedPageRandom(Memspace &memspace, Address address,
            std::vector<unsigned char> &expected_page, std::mt19937 &rng, std::uint32_t write_count)
        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            auto *page = static_cast<unsigned char *>(lock.modify());
            std::uniform_int_distribution<std::size_t> offset_dist(0, page_size - 1);
            std::uniform_int_distribution<unsigned int> value_dist(0, 255);
            for (std::uint32_t i = 0; i < write_count; ++i) {
                auto offset = offset_dist(rng);
                auto value = static_cast<unsigned char>(value_dist(rng));
                page[offset] = value;
                expected_page[offset] = value;
            }
        }
    };

    TEST_F( MetaSpaceTest, testMetaSpacePersistsFullDPAndReopens )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto address = memspace.alloc(page_size);
        fillPage(memspace, address, 0x42);

        ASSERT_TRUE(flushMeta(memspace, io));

        auto reopened = MetaSpace::create(page_size, sparse_pair, io);
        auto data = readPage(reopened, address);
        ASSERT_EQ(data, std::vector<unsigned char>(page_size, 0x42));
    }

    TEST_F( MetaSpaceTest, testMetaSpacePersistsDiffAndReopens )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto address = memspace.alloc(page_size);
        fillPage(memspace, address, 0x11);
        ASSERT_TRUE(flushMeta(memspace, io));

        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            auto *data = static_cast<unsigned char *>(lock.modify());
            data[17] = 0x22;
            data[1234] = 0x33;
        }
        ASSERT_TRUE(flushMeta(memspace, io));
        ASSERT_GT(io.getStats().second, 0u);

        auto reopened = MetaSpace::create(page_size, sparse_pair, io);
        auto data = readPage(reopened, address);
        ASSERT_EQ(data[0], 0x11);
        ASSERT_EQ(data[17], 0x22);
        ASSERT_EQ(data[1234], 0x33);
    }

    TEST_F( MetaSpaceTest, testMetaSpaceCapturesPreviousPageOnlyOnFirstDirtyMap )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto address = memspace.alloc(page_size);
        fillPage(memspace, address, 0x11);
        ASSERT_TRUE(flushMeta(memspace, io));

        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            auto *data = static_cast<unsigned char *>(lock.modify());
            data[17] = 0x22;
        }
        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            auto *data = static_cast<unsigned char *>(lock.modify());
            data[1234] = 0x33;
        }
        ASSERT_TRUE(flushMeta(memspace, io));

        auto reopened = MetaSpace::create(page_size, sparse_pair, io);
        auto data = readPage(reopened, address);
        ASSERT_EQ(data[0], 0x11);
        ASSERT_EQ(data[17], 0x22);
        ASSERT_EQ(data[1234], 0x33);
    }

    TEST_F( MetaSpaceTest, testMetaSpaceNoopCommitDoesNotAdvanceState )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto address = memspace.alloc(page_size);
        fillPage(memspace, address, 0x7f);
        ASSERT_TRUE(flushMeta(memspace, io));
        auto state_num = memspace.getStateNum();

        ASSERT_FALSE(flushMeta(memspace, io));
        ASSERT_EQ(memspace.getStateNum(), state_num);
    }

    TEST_F( MetaSpaceTest, testMetaSpaceReopenAllocatorGrowsFromLoadedHighWater )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto first = memspace.alloc(page_size);
        auto second = memspace.alloc(page_size);
        fillPage(memspace, first, 0x01);
        fillPage(memspace, second, 0x02);
        memspace.free(second);
        auto reused = memspace.alloc(page_size);
        ASSERT_EQ(reused, second);
        fillPage(memspace, reused, 0x03);
        ASSERT_TRUE(flushMeta(memspace, io));

        auto reopened = MetaSpace::create(page_size, sparse_pair, io);
        auto next = reopened.alloc(page_size);
        ASSERT_EQ(next.getOffset(), second.getOffset() + page_size);
    }

    TEST_F( MetaSpaceTest, testMetaSpaceReopenAllocatorRestoresSparseHoles )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto first = memspace.alloc(page_size);
        auto second = memspace.alloc(page_size);
        auto third = memspace.alloc(page_size);
        fillPage(memspace, first, 0x01);
        fillPage(memspace, third, 0x03);
        ASSERT_TRUE(flushMeta(memspace, io));

        auto reopened = MetaSpace::create(page_size, sparse_pair, io);
        auto reused = reopened.alloc(page_size);
        ASSERT_EQ(reused, second);
    }

    TEST_F( MetaSpaceTest, testSparsePairDeploysOnMetaSpaceWith16KBPageSize )
    {
        constexpr std::size_t large_page_size = 16 << 10;

        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair(large_page_size);
        SparsePair mapping_sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file, large_page_size);
        auto meta_space = MetaSpace::create(large_page_size, mapping_sparse_pair, io);
        auto meta_pair = createPairFromMetaSpace(meta_space);

        using PageModel = std::map<std::uint32_t, std::uint64_t>;
        std::map<std::uint64_t, PageModel> sparse_model;
        std::map<std::uint64_t, PageModel> diff_model;
        std::vector<std::uint64_t> pages_with_sparse_ops;
        std::vector<std::uint64_t> pages_with_diff_ops;

        std::mt19937_64 rng(0xdb0016);
        std::uniform_int_distribution<std::uint64_t> page_dist(0, 511);
        std::bernoulli_distribution sparse_op_dist(0.62);

        std::uint32_t state_num = 1;
        std::uint64_t storage_page_num = 101;
        constexpr std::size_t op_count = 1000;

        SparsePair cut(SparsePair::tag_create(), meta_pair);
        for (std::size_t i = 0; i < op_count; ++i) {
            auto page_num = page_dist(rng);
            storage_page_num += 1 + (rng() % 7);

            if (sparse_op_dist(rng)) {
                cut.getSparseIndex().emplace(page_num, state_num, storage_page_num);
                if (sparse_model[page_num].empty()) {
                    pages_with_sparse_ops.push_back(page_num);
                }
                sparse_model[page_num][state_num] = storage_page_num;
            } else {
                cut.getDiffIndex().insert(page_num, state_num, storage_page_num, (rng() % 23) == 0);
                if (diff_model[page_num].empty()) {
                    pages_with_diff_ops.push_back(page_num);
                }
                diff_model[page_num][state_num] = storage_page_num;
            }

            ++state_num;
        }
        cut.commit();

        ASSERT_TRUE(flushMeta(meta_space, io));

        auto reopened_meta_space = MetaSpace::create(large_page_size, mapping_sparse_pair, io);
        auto reopened_meta_pair = createPairFromMetaSpace(reopened_meta_space);
        SparsePair reopened(reopened_meta_pair, AccessType::READ_WRITE);

        ASSERT_GT(reopened.size(), 500u);
        ASSERT_EQ(reopened.getMaxStateNum(), state_num - 1);

        for (const auto &[page_num, states]: sparse_model) {
            for (const auto &[expected_state_num, expected_storage_page_num]: states) {
                auto sparse_item = reopened.getSparseIndex().lookup(page_num, expected_state_num);
                ASSERT_TRUE(sparse_item);
                ASSERT_EQ(sparse_item.m_state_num, expected_state_num);
                ASSERT_EQ(sparse_item.m_storage_page_num, expected_storage_page_num);
            }
        }

        for (const auto &[page_num, states]: diff_model) {
            for (const auto &[expected_state_num, expected_storage_page_num]: states) {
                ASSERT_EQ(reopened.getDiffIndex().findLower(page_num, expected_state_num), expected_state_num);

                auto diff_item = reopened.getDiffIndex().findUpper(page_num, expected_state_num);
                ASSERT_TRUE(diff_item);
                auto actual_storage_page_num = findDiffStoragePage(diff_item, expected_state_num);
                ASSERT_TRUE(actual_storage_page_num);
                ASSERT_EQ(*actual_storage_page_num, expected_storage_page_num);
            }
        }

        std::shuffle(pages_with_sparse_ops.begin(), pages_with_sparse_ops.end(), rng);
        for (std::size_t i = 0; i < std::min<std::size_t>(pages_with_sparse_ops.size(), 256); ++i) {
            auto page_num = pages_with_sparse_ops[i];
            auto query_state_num = static_cast<std::uint32_t>(rng() % state_num);
            auto expected_it = sparse_model[page_num].upper_bound(query_state_num);
            auto sparse_item = reopened.getSparseIndex().lookup(page_num, query_state_num);
            if (expected_it == sparse_model[page_num].begin()) {
                ASSERT_FALSE(sparse_item);
            } else {
                --expected_it;
                ASSERT_TRUE(sparse_item);
                ASSERT_EQ(sparse_item.m_state_num, expected_it->first);
                ASSERT_EQ(sparse_item.m_storage_page_num, expected_it->second);
            }
        }

        std::shuffle(pages_with_diff_ops.begin(), pages_with_diff_ops.end(), rng);
        for (std::size_t i = 0; i < std::min<std::size_t>(pages_with_diff_ops.size(), 256); ++i) {
            auto page_num = pages_with_diff_ops[i];
            auto query_state_num = static_cast<std::uint32_t>(rng() % state_num);
            auto expected_lower_it = diff_model[page_num].upper_bound(query_state_num);
            auto expected_upper_it = diff_model[page_num].lower_bound(query_state_num);

            if (expected_lower_it == diff_model[page_num].begin()) {
                ASSERT_EQ(reopened.getDiffIndex().findLower(page_num, query_state_num), 0u);
            } else {
                --expected_lower_it;
                ASSERT_EQ(reopened.getDiffIndex().findLower(page_num, query_state_num), expected_lower_it->first);
            }

            auto diff_item = reopened.getDiffIndex().findUpper(page_num, query_state_num);
            if (expected_upper_it == diff_model[page_num].end()) {
                ASSERT_FALSE(diff_item);
            } else {
                ASSERT_TRUE(diff_item);
                auto actual_storage_page_num = findDiffStoragePage(diff_item, expected_upper_it->first);
                ASSERT_TRUE(actual_storage_page_num);
                ASSERT_EQ(*actual_storage_page_num, expected_upper_it->second);
            }
        }
    }

    TEST_F( MetaSpaceTest, testMetaSpaceCompactionRewritesDiffBackedPageAndReopens )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto address = memspace.alloc(page_size);
        fillPage(memspace, address, 0x11);
        ASSERT_TRUE(flushMeta(memspace, io));

        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            auto *data = static_cast<unsigned char *>(lock.modify());
            data[17] = 0x22;
            data[1234] = 0x33;
        }
        ASSERT_TRUE(flushMeta(memspace, io));
        ASSERT_GT(sparse_pair.getDiffIndex().size(), 0u);
        auto diff_item = sparse_pair.getDiffIndex().findUpper(address.getOffset() / page_size, memspace.getStateNum());
        ASSERT_TRUE(diff_item);
        auto stale_diff_storage_page = findDiffStoragePage(diff_item, memspace.getStateNum());
        ASSERT_TRUE(stale_diff_storage_page);

        ASSERT_TRUE(compactMeta(memspace, io));
        ASSERT_GT(sparse_pair.getDiffIndex().size(), 0u);

        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            static_cast<unsigned char *>(lock.modify())[2048] = 0x44;
        }
        ASSERT_TRUE(flushMeta(memspace, io));
        auto next_diff_item = sparse_pair.getDiffIndex().findUpper(address.getOffset() / page_size, memspace.getStateNum());
        ASSERT_TRUE(next_diff_item);
        auto next_diff_storage_page = findDiffStoragePage(next_diff_item, memspace.getStateNum());
        ASSERT_TRUE(next_diff_storage_page);
        ASSERT_NE(*next_diff_storage_page, *stale_diff_storage_page);

        auto reopened = MetaSpace::create(page_size, sparse_pair, io);
        auto data = readPage(reopened, address);
        ASSERT_EQ(data[0], 0x11);
        ASSERT_EQ(data[17], 0x22);
        ASSERT_EQ(data[1234], 0x33);
        ASSERT_EQ(data[2048], 0x44);
    }

    TEST_F( MetaSpaceTest, testMetaSpaceCompactionReusesStaleFullDP )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto address = memspace.alloc(page_size);
        fillPage(memspace, address, 0x10);
        ASSERT_TRUE(flushMeta(memspace, io));
        auto initial_item = sparse_pair.getSparseIndex().lookup(address.getOffset() / page_size, memspace.getStateNum());
        ASSERT_TRUE(initial_item);
        auto stale_storage_page = initial_item.m_storage_page_num;
        ASSERT_NE(stale_storage_page, 0u);

        ASSERT_TRUE(compactMeta(memspace, io));
        auto first_compact_item = sparse_pair.getSparseIndex().lookup(address.getOffset() / page_size, memspace.getStateNum());
        ASSERT_TRUE(first_compact_item);
        ASSERT_NE(first_compact_item.m_storage_page_num, stale_storage_page);

        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            static_cast<unsigned char *>(lock.modify())[0] = 0x20;
        }
        ASSERT_TRUE(flushMeta(memspace, io));
        ASSERT_TRUE(compactMeta(memspace, io));

        auto second_compact_item = sparse_pair.getSparseIndex().lookup(address.getOffset() / page_size, memspace.getStateNum());
        ASSERT_TRUE(second_compact_item);
        ASSERT_NE(second_compact_item.m_storage_page_num, 0u);

        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            static_cast<unsigned char *>(lock.modify())[0] = 0x30;
        }
        ASSERT_TRUE(flushMeta(memspace, io));
        ASSERT_TRUE(compactMeta(memspace, io));

        auto third_compact_item = sparse_pair.getSparseIndex().lookup(address.getOffset() / page_size, memspace.getStateNum());
        ASSERT_TRUE(third_compact_item);
        ASSERT_NE(third_compact_item.m_storage_page_num, 0u);

        auto reopened = MetaSpace::create(page_size, sparse_pair, io);
        auto data = readPage(reopened, address);
        ASSERT_EQ(data[0], 0x30);
    }

    TEST_F( MetaSpaceTest, testMetaSpaceCompactionDoesNotOverwriteCurrentHeadFullDP )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto address = memspace.alloc(page_size);
        fillPage(memspace, address, 0x10);
        ASSERT_TRUE(flushMeta(memspace, io));

        auto page_num = address.getOffset() / page_size;
        auto head_state_num = memspace.getStateNum();
        auto head_item = sparse_pair.getSparseIndex().lookup(page_num, head_state_num);
        ASSERT_TRUE(head_item);
        auto head_storage_page_num = head_item.m_storage_page_num;

        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            static_cast<unsigned char *>(lock.modify())[0] = 0x20;
        }

        ASSERT_TRUE(compactMeta(memspace, io));
        auto current_head_data = readStoragePage(io, head_storage_page_num);
        ASSERT_EQ(current_head_data, std::vector<unsigned char>(page_size, 0x10));
    }

    TEST_F( MetaSpaceTest, testMetaSpaceCompactionLeavesCurrentHeadDiffReadable )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto address = memspace.alloc(page_size);
        fillPage(memspace, address, 0x11);
        ASSERT_TRUE(flushMeta(memspace, io));

        {
            auto lock = memspace.getPrefix().mapRange(address.getOffset(), page_size, { AccessOptions::write });
            auto *data = static_cast<unsigned char *>(lock.modify());
            data[17] = 0x22;
            data[1234] = 0x33;
        }
        ASSERT_TRUE(flushMeta(memspace, io));

        auto page_num = address.getOffset() / page_size;
        auto head_state_num = memspace.getStateNum();
        SparseIndexQuery query(sparse_pair.getSparseIndex(), sparse_pair.getDiffIndex(), page_num, head_state_num);
        ASSERT_FALSE(query.empty());
        std::vector<unsigned char> current_head_buffer(page_size);
        io.read(query.first(), current_head_buffer.data());
        StateNumType diff_state_num = 0;
        std::uint64_t diff_storage_page_num = 0;
        ASSERT_TRUE(query.next(diff_state_num, diff_storage_page_num));
        ASSERT_EQ(diff_state_num, head_state_num);

        ASSERT_TRUE(compactMeta(memspace, io));

        io.applyFrom(diff_storage_page_num, current_head_buffer.data(), { page_num, diff_state_num });
        ASSERT_EQ(current_head_buffer[0], 0x11);
        ASSERT_EQ(current_head_buffer[17], 0x22);
        ASSERT_EQ(current_head_buffer[1234], 0x33);
    }

    TEST_F( MetaSpaceTest, testMetaSpaceCompactionReusesThirdFullDPVersion )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        SparsePair sparse_pair(page_size);

        auto io = createIO(file);
        constexpr std::uint64_t page_num = 1;
        bool is_first_page = false;

        std::vector<unsigned char> oldest_buffer(page_size, 0x41);
        auto oldest_storage_page_num = io.append(oldest_buffer.data(), &is_first_page);
        sparse_pair.getSparseIndex().emplace(page_num, 1, oldest_storage_page_num);

        std::vector<unsigned char> previous_buffer(page_size, 0x42);
        auto previous_storage_page_num = io.append(previous_buffer.data(), &is_first_page);
        sparse_pair.getSparseIndex().emplace(page_num, 2, previous_storage_page_num);

        std::vector<unsigned char> head_buffer(page_size, 0x43);
        auto head_storage_page_num = io.append(head_buffer.data(), &is_first_page);
        sparse_pair.getSparseIndex().emplace(page_num, 3, head_storage_page_num);
        sparse_pair.commit();

        MetaPrefix prefix(page_size, sparse_pair);
        ASSERT_EQ(prefix.getStateNum(), 3u);

        ASSERT_TRUE(compact(prefix, io));

        auto compacted_item = sparse_pair.getSparseIndex().lookup(page_num, prefix.getStateNum());
        ASSERT_TRUE(compacted_item);
        ASSERT_EQ(compacted_item.m_storage_page_num, oldest_storage_page_num);
    }

    TEST_F( MetaSpaceTest, testMetaSpaceCompactionPersistsDirtyPageWithoutPriorFlush )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        auto address = memspace.alloc(page_size);
        fillPage(memspace, address, 0x55);

        ASSERT_TRUE(compactMeta(memspace, io));
        auto item = sparse_pair.getSparseIndex().lookup(address.getOffset() / page_size, memspace.getStateNum());
        ASSERT_TRUE(item);
        ASSERT_NE(item.m_storage_page_num, 0u);

        auto reopened = MetaSpace::create(page_size, sparse_pair, io);
        auto data = readPage(reopened, address);
        ASSERT_EQ(data, std::vector<unsigned char>(page_size, 0x55));
    }

    TEST_F( MetaSpaceTest, testMetaSpaceCompactionBiggerSimulatedWorkload )
    {
        CFile::create(file_name, {});
        CFile file(file_name, AccessType::READ_WRITE);
        auto mapping_pair = createMappingPair();
        SparsePair sparse_pair(SparsePair::tag_create(), mapping_pair);

        auto io = createIO(file);
        auto memspace = MetaSpace::create(page_size, sparse_pair, io);
        constexpr std::size_t page_count = 640;
        std::vector<Address> addresses;
        std::vector<std::vector<unsigned char> > expected_pages;
        std::vector<bool> dirty_before_second_compact(page_count, false);
        addresses.reserve(page_count);
        expected_pages.reserve(page_count);
        std::mt19937 rng(0xDB005EED);
        std::uniform_int_distribution<std::size_t> page_dist(0, page_count - 1);
        std::uniform_int_distribution<std::uint32_t> sparse_write_count_dist(1, 12);
        std::uniform_int_distribution<std::uint32_t> dense_write_count_dist(16, 96);

        for (std::size_t i = 0; i < page_count; ++i) {
            auto address = memspace.alloc(page_size);
            addresses.push_back(address);
            ASSERT_NE(address.getOffset(), 0u) << "page index " << i;
            expected_pages.emplace_back(page_size, static_cast<unsigned char>((i + 1) & 0xFF));
            fillPage(memspace, address, expected_pages.back()[0]);
        }
        ASSERT_TRUE(flushMeta(memspace, io));

        for (std::uint32_t round = 1; round <= 9; ++round) {
            auto operation_count = page_count / 2 + round * 17;
            for (std::size_t op = 0; op < operation_count; ++op) {
                auto page_index = page_dist(rng);
                patchExpectedPageRandom(
                    memspace, addresses[page_index], expected_pages[page_index], rng, sparse_write_count_dist(rng)
                );
            }
            ASSERT_TRUE(flushMeta(memspace, io));
        }
        ASSERT_GT(sparse_pair.getDiffIndex().size(), 0u);

        for (std::size_t i = 0; i < 16; ++i) {
            auto page_index = page_dist(rng);
            ASSERT_EQ(readPage(memspace, addresses[page_index]), expected_pages[page_index])
                << "pre-compact page index " << page_index;
        }
        ASSERT_TRUE(compactMeta(memspace, io));
        ASSERT_EQ(sparse_pair.getSparseIndex().size(), page_count);
        for (std::size_t i = 0; i < 16; ++i) {
            auto page_index = page_dist(rng);
            ASSERT_EQ(readPage(memspace, addresses[page_index]), expected_pages[page_index])
                << "post-first-compact page index " << page_index;
        }

        for (std::uint32_t round = 10; round <= 12; ++round) {
            auto operation_count = page_count / 3 + round * 23;
            for (std::size_t op = 0; op < operation_count; ++op) {
                auto page_index = page_dist(rng);
                patchExpectedPageRandom(
                    memspace, addresses[page_index], expected_pages[page_index], rng, dense_write_count_dist(rng)
                );
                if (round == 12) {
                    dirty_before_second_compact[page_index] = true;
                }
            }
            if (round != 12) {
                ASSERT_TRUE(flushMeta(memspace, io));
            }
        }

        for (std::size_t page_index = 0; page_index < page_count; ++page_index) {
            ASSERT_EQ(readPage(memspace, addresses[page_index]), expected_pages[page_index])
                << "pre-second-compact page index " << page_index;
        }
        ASSERT_TRUE(compactMeta(memspace, io));
        ASSERT_EQ(sparse_pair.getSparseIndex().size(), page_count);
        for (std::size_t i = 0; i < 16; ++i) {
            auto page_index = page_dist(rng);
            ASSERT_EQ(readPage(memspace, addresses[page_index]), expected_pages[page_index])
                << "post-second-compact page index " << page_index;
        }
        for (std::size_t page_index = 0; page_index < page_count; ++page_index) {
            auto item = sparse_pair.getSparseIndex().lookup(
                addresses[page_index].getOffset() / page_size, memspace.getStateNum()
            );
            ASSERT_TRUE(item) << "page index " << page_index;
            ASSERT_EQ(readStoragePage(io, item.m_storage_page_num), expected_pages[page_index])
                << "storage page check page index " << page_index
                << " dirty before second compact " << dirty_before_second_compact[page_index];
        }

        auto reopened = MetaSpace::create(page_size, sparse_pair, io);
        for (std::size_t i = 0; i < page_count; ++i) {
            auto data = readPage(reopened, addresses[i]);
            ASSERT_EQ(data, expected_pages[i]) << "page index " << i << " address " << addresses[i].getOffset();
        }

        std::vector<std::uint64_t> allocated_addresses;
        dynamic_cast<MetaPrefix &>(reopened.getPrefix()).forAllocatedAddresses([&](std::uint64_t address) {
            allocated_addresses.push_back(address);
        });

        ASSERT_EQ(allocated_addresses.size(), addresses.size());
        for (std::size_t i = 0; i < addresses.size(); ++i) {
            ASSERT_EQ(allocated_addresses[i], addresses[i].getOffset());
        }
    }

}
