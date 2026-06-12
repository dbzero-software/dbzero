// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/utils.hpp>
#include <dbzero/core/dram/DRAM_Allocator.hpp>
#include <dbzero/core/dram/DRAM_Prefix.hpp>
#include <dbzero/core/dram/MetaSpace.hpp>
#include <dbzero/core/storage/CFile.hpp>
#include <dbzero/core/storage/Diff_IO.hpp>
#include <dbzero/core/storage/RandomIO_Stream.hpp>
#include <dbzero/core/storage/SparsePairQuery.hpp>

using namespace db0;
using namespace db0::tests;

namespace tests

{

    class SparsePairQueryTest: public testing::Test
    {
    public:
        static constexpr const char *file_name = "sparse-pair-query-test.db0";
        static constexpr std::size_t page_size = 4096;

        void SetUp() override
        {
            drop(file_name);
            CFile::create(file_name, {});
        }

        void TearDown() override
        {
            drop(file_name);
        }

        static DRAM_Pair createMappingPair()
        {
            return {
                std::make_shared<DRAM_Prefix>(page_size),
                std::make_shared<DRAM_Allocator>(page_size)
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
    };

    TEST_F( SparsePairQueryTest , testSinglePageUsesSinglePageMapping )
    {
        unsigned int single_page_mapping_calls = 0;
        unsigned int bucket_mapping_calls = 0;
        StorageOptions options;
        options.m_storage_slab_bucketing = [&](std::uint64_t address) {
            ++single_page_mapping_calls;
            return static_cast<std::uint32_t>(address / page_size);
        };
        options.m_storage_slab_bucket = [&](std::uint64_t) {
            ++bucket_mapping_calls;
            return StorageOptions::StorageSlabBucket { 9u, 0u, 10u };
        };

        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        SparsePair root_pair(SparsePair::tag_create(), createMappingPair());
        auto meta_space = MS_MetaSpace::create(page_size, root_pair, stream);
        SparsePairManager manager(meta_space, AccessType::READ_WRITE, {});

        SparsePairQuery<true> query(options, page_size, 3, 4, manager);

        ASSERT_TRUE(query.hasNext());
        ASSERT_EQ(query.pageNum(), 3u);
        ASSERT_EQ(query.currentSparsePair(), nullptr);
        ASSERT_EQ(query.slotId(), 3u);
        ASSERT_EQ(single_page_mapping_calls, 1u);
        ASSERT_EQ(bucket_mapping_calls, 0u);
    }

    TEST_F( SparsePairQueryTest , testMultiPageCachesSparsePairWithinBucket )
    {
        unsigned int bucket_mapping_calls = 0;
        StorageOptions options;
        options.m_storage_slab_bucketing = [](std::uint64_t) {
            return 0u;
        };
        options.m_storage_slab_bucket = [&](std::uint64_t) {
            ++bucket_mapping_calls;
            return StorageOptions::StorageSlabBucket { 7u, 0u, 16u };
        };

        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        SparsePair root_pair(SparsePair::tag_create(), createMappingPair());
        auto meta_space = MS_MetaSpace::create(page_size, root_pair, stream);
        SparsePairManager manager(meta_space, AccessType::READ_WRITE, {});
        SparsePairQuery<true> query(options, page_size, 4, 6, manager);

        ASSERT_EQ(bucket_mapping_calls, 1u);
        ASSERT_EQ(query.slotId(), 7u);
        ASSERT_EQ(query.currentSparsePair(), nullptr);
        ++query;
        ASSERT_EQ(query.slotId(), 7u);
        ASSERT_EQ(query.currentSparsePair(), nullptr);
        ASSERT_EQ(bucket_mapping_calls, 1u);
    }

    TEST_F( SparsePairQueryTest , testMultiPageRefreshesAtBucketBoundary )
    {
        unsigned int bucket_mapping_calls = 0;
        StorageOptions options;
        options.m_storage_slab_bucketing = [](std::uint64_t) {
            return 0u;
        };
        options.m_storage_slab_bucket = [&](std::uint64_t address) {
            ++bucket_mapping_calls;
            auto page_num = address / page_size;
            return StorageOptions::StorageSlabBucket {
                static_cast<std::uint32_t>(page_num), page_num, page_num + 1
            };
        };

        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        SparsePair root_pair(SparsePair::tag_create(), createMappingPair());
        auto meta_space = MS_MetaSpace::create(page_size, root_pair, stream);
        SparsePairManager manager(meta_space, AccessType::READ_WRITE, {});
        SparsePairQuery<true> query(options, page_size, 0, 2, manager);

        ASSERT_EQ(query.currentSparsePair(), nullptr);
        ++query;
        ASSERT_EQ(query.currentSparsePair(), nullptr);
        ASSERT_EQ(bucket_mapping_calls, 2u);
    }

    TEST_F( SparsePairQueryTest , testWriteQueryCreatesSparsePair )
    {
        StorageOptions options;
        options.m_storage_slab_bucketing = [](std::uint64_t) {
            return 0u;
        };

        CFile file(file_name, AccessType::READ_WRITE);
        auto io = createIO(file);
        auto stream = createStream(io);
        SparsePair root_pair(SparsePair::tag_create(), createMappingPair());
        auto meta_space = MS_MetaSpace::create(page_size, root_pair, stream);
        SparsePairManager manager(meta_space, AccessType::READ_WRITE, {});

        SparsePairQuery<false> query(options, page_size, 0, 1, manager);

        auto &sparse_pair = query.currentOrCreateSparsePair();
        ASSERT_EQ(manager.tryGetExisting(0), &sparse_pair);
    }

}
