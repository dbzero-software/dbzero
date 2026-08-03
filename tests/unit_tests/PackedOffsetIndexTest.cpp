// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>

#include <dbzero/core/serialization/bounded_buf_t.hpp>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/object_model/object/o_packed_offset_index.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace tests
{
    using namespace db0;
    using namespace db0::object_model;

    static_assert(
        std::is_base_of<
            db0::o_ext<o_packed_offset_index, db0::o_list<o_packed_offset_group, true>, 0, false>,
            o_packed_offset_index
        >::value,
        "Derived overlaid types must use o_ext"
    );

    class PackedOffsetIndexTest: public MemspaceTestBase
    {
    };

    static void throwDecodeError()
    {
        throw std::runtime_error("decode error");
    }

    static void assertIndexConstructionStaysWithinMeasuredSize(const std::vector<std::uint64_t> &offsets)
    {
        constexpr auto guardSize = std::size_t { 64 };
        constexpr auto guardByte = std::byte { 0xa5 };
        auto measured = o_packed_offset_index::measure(offsets);
        std::vector<std::byte> storage(guardSize + measured + guardSize, guardByte);
        auto *objectBegin = storage.data() + guardSize;
        auto *objectEnd = objectBegin + measured;

        auto &index = o_packed_offset_index::__new(objectBegin, offsets);

        ASSERT_EQ(index.sizeOf(), measured);
        ASSERT_TRUE(std::all_of(storage.data(), objectBegin, [](std::byte value) {
            return value == guardByte;
        }));
        ASSERT_TRUE(std::all_of(objectEnd, storage.data() + storage.size(), [](std::byte value) {
            return value == guardByte;
        }));
    }

    static void assertGroupConstructionStaysWithinMeasuredSize(const std::vector<std::uint64_t> &values)
    {
        constexpr auto guardSize = std::size_t { 64 };
        constexpr auto guardByte = std::byte { 0xa5 };
        auto measured = o_packed_offset_group::measure(values.data(), values.data() + values.size());
        std::vector<std::byte> storage(guardSize + measured + guardSize, guardByte);
        auto *objectBegin = storage.data() + guardSize;
        auto *objectEnd = objectBegin + measured;

        auto &group = o_packed_offset_group::__new(objectBegin, values.data(), values.data() + values.size());

        ASSERT_EQ(group.sizeOf(), measured);
        ASSERT_TRUE(std::all_of(storage.data(), objectBegin, [](std::byte value) {
            return value == guardByte;
        }));
        ASSERT_TRUE(std::all_of(objectEnd, storage.data() + storage.size(), [](std::byte value) {
            return value == guardByte;
        }));
    }

    TEST_F( PackedOffsetIndexTest, testEmptyIndex )
    {
        auto memspace = getMemspace();
        v_object<o_packed_offset_index> index(memspace, std::vector<std::uint64_t> {});

        ASSERT_EQ(index->size(), 0u);
        ASSERT_TRUE(index->empty());
        ASSERT_FALSE(index->contains(1));
        ASSERT_EQ(index->sizeOf(), o_packed_offset_index::measure({}));
    }

    TEST_F( PackedOffsetIndexTest, testSingleAndMisses )
    {
        auto memspace = getMemspace();
        v_object<o_packed_offset_index> index(memspace, std::vector<std::uint64_t> { 42 });

        ASSERT_EQ(index->size(), 1u);
        ASSERT_TRUE(index->contains(42));
        ASSERT_FALSE(index->contains(41));
        ASSERT_FALSE(index->contains(43));
    }

    TEST_F( PackedOffsetIndexTest, testMultiGroupExactLookup )
    {
        std::vector<std::uint64_t> offsets {
            1, 2, 3, 127, 128, 16'383, 16'384, 2'097'151, 2'097'152, 268'435'455, 268'435'456
        };
        auto memspace = getMemspace();
        v_object<o_packed_offset_index> index(memspace, offsets);

        ASSERT_EQ(index->size(), offsets.size());
        for (auto offset: offsets) {
            ASSERT_TRUE(index->contains(offset)) << offset;
        }
        ASSERT_FALSE(index->contains(0));
        ASSERT_FALSE(index->contains(129));
        ASSERT_FALSE(index->contains(2'097'153));
        ASSERT_FALSE(index->contains(999'999'999));
    }

    TEST_F( PackedOffsetIndexTest, testIteratorYieldsOffsetsAcrossPackedGroups )
    {
        std::vector<std::uint64_t> offsets {
            1, 2, 127, 128, 16'383, 16'384, 2'097'151, 2'097'152, 268'435'456
        };
        auto memspace = getMemspace();
        v_object<o_packed_offset_index> index(memspace, offsets);

        std::vector<std::uint64_t> result;
        for (auto it = index->begin(); it != index->end(); ++it) {
            result.push_back(*it);
        }

        ASSERT_EQ(result, offsets);
    }

    TEST_F( PackedOffsetIndexTest, testWidthBoundaryGroupsUseSortedInput )
    {
        std::vector<std::uint64_t> offsets {
            1,
            127,
            128,
            16'383,
            16'384,
            2'097'151,
            2'097'152,
            std::numeric_limits<std::uint64_t>::max()
        };
        auto memspace = getMemspace();
        v_object<o_packed_offset_index> index(memspace, offsets);

        for (auto offset: offsets) {
            ASSERT_TRUE(index->contains(offset)) << offset;
        }
        ASSERT_FALSE(index->contains(0));
        ASSERT_FALSE(index->contains(126));
        ASSERT_FALSE(index->contains(129));
        ASSERT_FALSE(index->contains(16'385));
        ASSERT_FALSE(index->contains(std::numeric_limits<std::uint64_t>::max() - 1));
    }

    TEST_F( PackedOffsetIndexTest, testContainsUsesWidthSelectedGroup )
    {
        std::vector<std::uint64_t> offsets {
            1, 3, 127,
            128, 130, 16'383,
            16'384, 16'386, 2'097'151,
            2'097'152, 2'097'154
        };
        auto memspace = getMemspace();
        v_object<o_packed_offset_index> index(memspace, offsets);

        ASSERT_FALSE(index->contains(2));
        ASSERT_FALSE(index->contains(129));
        ASSERT_FALSE(index->contains(16'385));
        ASSERT_FALSE(index->contains(2'097'153));
        ASSERT_TRUE(index->contains(127));
        ASSERT_TRUE(index->contains(128));
        ASSERT_TRUE(index->contains(16'384));
        ASSERT_TRUE(index->contains(2'097'154));
    }

    TEST_F( PackedOffsetIndexTest, testGroupContainsMatchesLargeCollectionRandomLookups )
    {
        std::vector<std::uint64_t> values;
        values.reserve(160);
        for (std::uint64_t i = 0; i < 160; ++i) {
            values.push_back(128 + (i * 2));
        }
        auto memspace = getMemspace();
        v_object<o_packed_offset_group> group(memspace, values.data(), values.data() + values.size());
        std::mt19937 rng(0xDB0);
        std::uniform_int_distribution<std::size_t> hitDistribution(0, values.size() - 1);
        std::uniform_int_distribution<std::uint64_t> missDistribution(0, values.size() - 2);

        ASSERT_FALSE(group->contains(127));
        ASSERT_FALSE(group->contains(16'384));

        for (auto i = 0; i < 512; ++i) {
            auto hitIndex = hitDistribution(rng);
            ASSERT_TRUE(group->contains(values[hitIndex])) << values[hitIndex];

            auto missValue = values[missDistribution(rng)] + 1;
            ASSERT_FALSE(group->contains(missValue)) << missValue;
        }
    }

    TEST_F( PackedOffsetIndexTest, testConstructionDoesNotWriteOutsideMeasuredSize )
    {
        assertIndexConstructionStaysWithinMeasuredSize({});
        assertIndexConstructionStaysWithinMeasuredSize({ 42 });
        assertIndexConstructionStaysWithinMeasuredSize({
            1,
            127,
            128,
            16'383,
            16'384,
            2'097'151,
            2'097'152,
            268'435'455,
            268'435'456,
            std::numeric_limits<std::uint64_t>::max()
        });

        std::vector<std::uint64_t> denseOffsets;
        denseOffsets.reserve(512);
        for (std::uint64_t i = 0; i < 512; ++i) {
            denseOffsets.push_back(128 + (i * 3));
        }
        assertGroupConstructionStaysWithinMeasuredSize(denseOffsets);
        assertIndexConstructionStaysWithinMeasuredSize(denseOffsets);
    }

    TEST_F( PackedOffsetIndexTest, testCompactRepresentation )
    {
        std::vector<std::uint64_t> oneByteOffsets { 1, 2, 3, 4 };
        std::vector<std::uint64_t> mixedOffsets { 1, 128, 16'384, 2'097'152, 268'435'456 };
        auto memspace = getMemspace();

        v_object<o_packed_offset_index> oneByteIndex(memspace, oneByteOffsets);
        v_object<o_packed_offset_index> mixedIndex(memspace, mixedOffsets);

        ASSERT_EQ(oneByteIndex->sizeOf(), 7u);
        ASSERT_LT(oneByteIndex->sizeOf(), mixedIndex->sizeOf());
        ASSERT_EQ(oneByteIndex->sizeOf(), o_packed_offset_index::safeSizeOf(
            reinterpret_cast<const std::byte *>(oneByteIndex.getData())
        ));
        ASSERT_EQ(mixedIndex->sizeOf(), o_packed_offset_index::safeSizeOf(
            reinterpret_cast<const std::byte *>(mixedIndex.getData())
        ));
    }

    TEST_F( PackedOffsetIndexTest, testSafeSizeOfValidatesTruncatedData )
    {
        std::vector<std::uint64_t> offsets { 5, 128, 129 };
        auto memspace = getMemspace();
        v_object<o_packed_offset_index> index(memspace, offsets);
        auto begin = reinterpret_cast<const std::byte *>(index.getData());
        auto size = index->sizeOf();

        ASSERT_EQ(o_packed_offset_index::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size)), size);
        ASSERT_THROW(
            o_packed_offset_index::safeSizeOf(const_bounded_buf_t(throwDecodeError, begin, begin + size - 1)),
            std::runtime_error
        );
    }
}
