// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/serialization/Serializable.hpp>
#include <dbzero/core/serialization/bounded_buf_t.hpp>
#include <dbzero/core/serialization/list.hpp>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/collections/b_index/mb_index.hpp>

#include <stdexcept>

namespace tests

{
    
    using TypeOfType = decltype(db0::serial::typeId<void>());

    TEST( SerializationTest , testSerialWriteAndReadTypeIds )
    {
        std::vector<std::byte> buf;
        db0::serial::write(buf, db0::serial::typeId<int>());
        db0::serial::write(buf, db0::serial::typeId<std::string>());
        // complex type
        db0::serial::write(buf, db0::MorphingBIndex<std::uint64_t>::getSerialTypeId());

        auto iter = buf.cbegin(), end = buf.cend();
        ASSERT_EQ(db0::serial::read<TypeOfType>(iter, end), db0::serial::typeId<int>());
        ASSERT_EQ(db0::serial::read<TypeOfType>(iter, end), db0::serial::typeId<std::string>());
        ASSERT_EQ(db0::serial::read<TypeOfType>(iter, end), db0::MorphingBIndex<std::uint64_t>::getSerialTypeId());
        // reading past the end of the buffer should throw
        ASSERT_ANY_THROW(db0::serial::read<TypeOfType>(iter, end));
    }

    TEST( SerializationTest , testRegularOListKeepsFixedSizeHeader )
    {
        using List = db0::o_list<db0::o_simple<std::uint32_t> >;
        std::vector<std::uint32_t> values { 10, 20, 30 };
        std::vector<std::byte> buf(List::measure(values));

        auto &list = List::__new(buf.data(), values);

        ASSERT_EQ(db0::true_size_of<List>(), 8u);
        ASSERT_EQ(list.size(), 3u);
        ASSERT_EQ(list.sizeOf(), 8u + values.size() * sizeof(std::uint32_t));
        ASSERT_EQ(List::safeSizeOf(buf.data()), list.sizeOf());

        auto it = list.begin();
        ASSERT_EQ((*it).value(), 10u);
        ++it;
        ASSERT_EQ((*it).value(), 20u);
        ++it;
        ASSERT_EQ((*it).value(), 30u);
        ++it;
        ASSERT_EQ(it, list.end());
    }

    TEST( SerializationTest , testRegularOListSafeSizeOfUsesDeclaredSize )
    {
        using List = db0::o_list<db0::o_simple<std::uint32_t> >;
        std::vector<std::uint32_t> values { 10, 20, 30 };
        std::vector<std::byte> buf(List::measure(values));
        auto &list = List::__new(buf.data(), values);
        list.count = 4;

        auto throwOutOfBounds = []() {
            throw std::runtime_error("decode error");
        };
        db0::const_bounded_buf_t bounded(throwOutOfBounds, buf.data(), buf.data() + buf.size());

        ASSERT_EQ(List::safeSizeOf(bounded), list.sizeOf());
    }

    TEST( SerializationTest , testCompactOListStoresOnlyPackedSize )
    {
        using List = db0::o_list<db0::o_simple<std::uint32_t>, true>;
        std::vector<std::uint32_t> values { 10, 20, 30 };
        std::vector<std::byte> buf(List::measure(values));

        auto &list = List::__new(buf.data(), values);

        ASSERT_EQ(db0::true_size_of<List>(), 0u);
        ASSERT_EQ(list.size(), 3u);
        ASSERT_EQ(list.sizeOf(), 1u + values.size() * sizeof(std::uint32_t));
        ASSERT_EQ(List::safeSizeOf(buf.data()), list.sizeOf());

        auto it = list.begin();
        ASSERT_EQ((*it).value(), 10u);
        ++it;
        ASSERT_EQ((*it).value(), 20u);
        ++it;
        ASSERT_EQ((*it).value(), 30u);
        ++it;
        ASSERT_EQ(it, list.end());
    }

    TEST( SerializationTest , testCompactOListEmptyUsesOneByte )
    {
        using List = db0::o_list<db0::o_simple<std::uint32_t>, true>;
        std::vector<std::byte> buf(List::measure());

        auto &list = List::__new(buf.data());

        ASSERT_EQ(list.sizeOf(), 1u);
        ASSERT_EQ(list.size(), 0u);
        ASSERT_TRUE(list.empty());
        ASSERT_EQ(list.begin(), list.end());
        ASSERT_EQ(List::safeSizeOf(buf.data()), 1u);
    }

    TEST( SerializationTest , testCompactOListMeasureHandlesPackedSizeBoundary )
    {
        using List = db0::o_list<db0::o_simple<std::uint32_t>, true>;
        std::vector<std::uint32_t> values(32, 7);
        std::vector<std::byte> buf(List::measure(values));

        auto &list = List::__new(buf.data(), values);

        ASSERT_EQ(list.sizeOf(), 2u + values.size() * sizeof(std::uint32_t));
        ASSERT_EQ(list.size(), values.size());
        ASSERT_EQ(List::safeSizeOf(buf.data()), list.sizeOf());
    }
    
}
