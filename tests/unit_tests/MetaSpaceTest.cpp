// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <cstring>
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
            auto tail_function = [&file]() -> std::uint64_t {
                return file.size();
            };
            return Diff_IO(0, file, page_size, page_size * 16, page_size, 0, 1, tail_function, 0, 4);
        }

        static DRAM_Pair createMappingPair()
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

}
