// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/memory/Address.hpp>
#include <unordered_set>

namespace tests
{

    TEST( TagAddressTest , testDefaultIsInvalid )
    {
        db0::TagAddress cut;

        ASSERT_FALSE(cut.isValid());
        ASSERT_EQ(cut.getValue(), 0u);
        ASSERT_EQ(cut.getOffset(), 0u);
    }

    TEST( TagAddressTest , testConstructFromOffset )
    {
        auto cut = db0::TagAddress::fromOffset(12345);

        ASSERT_TRUE(cut.isValid());
        ASSERT_EQ(cut.getValue(), 12345u);
        ASSERT_EQ(cut.getOffset(), 12345u);
        ASSERT_EQ(cut.getAddress(), db0::Address::fromOffset(12345));
    }

    TEST( TagAddressTest , testComparisonUsesRawValue )
    {
        auto lhs = db0::TagAddress::fromValue(12345);
        auto rhs = db0::TagAddress::fromValue(12346);

        ASSERT_NE(lhs, rhs);
        ASSERT_LT(lhs, rhs);
    }

    TEST( TagAddressTest , testCastsUseRawValue )
    {
        auto tag = db0::TagAddress::fromValue(12345);

        db0::Address address = tag;
        std::uint64_t value = tag;

        ASSERT_EQ(address, db0::Address::fromOffset(12345));
        ASSERT_EQ(value, 12345u);
    }

    TEST( TagAddressTest , testHashUsesRawValue )
    {
        auto first = db0::TagAddress::fromValue(12345);
        auto second = db0::TagAddress::fromValue(12346);

        std::unordered_set<db0::TagAddress> values;
        values.insert(first);
        values.insert(second);

        ASSERT_EQ(values.size(), 2u);
        ASSERT_NE(std::hash<db0::TagAddress>()(first), std::hash<db0::TagAddress>()(second));
    }

    TEST( TagAddressTest , testLayoutMatchesUint64 )
    {
        ASSERT_EQ(sizeof(db0::TagAddress), sizeof(std::uint64_t));
        ASSERT_EQ(alignof(db0::TagAddress), alignof(db0::UniqueAddress));
    }

}
