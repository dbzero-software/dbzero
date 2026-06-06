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

}
