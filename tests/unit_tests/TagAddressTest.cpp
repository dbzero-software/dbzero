// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include <gtest/gtest.h>
#include <dbzero/core/memory/Address.hpp>
#include <unordered_set>

namespace tests
{

    TEST( TagAddressTest , testDefaultIsInvalid )
    {
        db0::TagAddress cut;

        ASSERT_FALSE(cut.isValid());
        ASSERT_FALSE(cut.isPassive());
        ASSERT_EQ(cut.getValue(), 0u);
        ASSERT_EQ(cut.getOffset(), 0u);
    }

    TEST( TagAddressTest , testConstructFromOffset )
    {
        auto cut = db0::TagAddress::fromOffset(12345);

        ASSERT_TRUE(cut.isValid());
        ASSERT_FALSE(cut.isPassive());
        ASSERT_EQ(cut.getValue(), 12345u);
        ASSERT_EQ(cut.getOffset(), 12345u);
        ASSERT_EQ(cut.getAddress(), db0::Address::fromOffset(12345));
    }

    TEST( TagAddressTest , testPassiveKeepsRawBitAndStripsLogicalOffset )
    {
        auto regular = db0::TagAddress::fromOffset(12345);
        auto passive = regular.asPassive();

        ASSERT_TRUE(passive.isValid());
        ASSERT_TRUE(passive.isPassive());
        ASSERT_EQ(passive.getValue(), 12345u | db0::TagAddress::PASSIVE_BIT);
        ASSERT_EQ(passive.getOffset(), 12345u);
        ASSERT_EQ(passive.getAddress(), db0::Address::fromOffset(12345));
        ASSERT_EQ(passive.asRegular().getValue(), 12345u);
    }

    TEST( TagAddressTest , testRawValueCanReopenPassiveAddress )
    {
        auto cut = db0::TagAddress::fromValue(12345u | db0::TagAddress::PASSIVE_BIT);

        ASSERT_TRUE(cut.isPassive());
        ASSERT_EQ(cut.getValue(), 12345u | db0::TagAddress::PASSIVE_BIT);
        ASSERT_EQ(cut.getOffset(), 12345u);
    }

    TEST( TagAddressTest , testRegularAndPassiveCompareAsSameLogicalAddress )
    {
        auto regular = db0::TagAddress::fromOffset(12345);
        auto passive = regular.asPassive();

        ASSERT_EQ(regular, passive);
        ASSERT_FALSE(regular < passive);
        ASSERT_FALSE(passive < regular);
        ASSERT_LT(regular, db0::TagAddress::fromOffset(12346));
    }

    TEST( TagAddressTest , testCastsClearPassiveBit )
    {
        auto passive = db0::TagAddress::fromOffset(12345).asPassive();

        db0::Address address = passive;
        std::uint64_t value = passive;

        ASSERT_EQ(address, db0::Address::fromOffset(12345));
        ASSERT_EQ(value, 12345u);
    }

    TEST( TagAddressTest , testHashUsesLogicalAddress )
    {
        auto regular = db0::TagAddress::fromOffset(12345);
        auto passive = regular.asPassive();

        std::unordered_set<db0::TagAddress> values;
        values.insert(regular);
        values.insert(passive);

        ASSERT_EQ(values.size(), 1u);
        ASSERT_EQ(std::hash<db0::TagAddress>()(regular), std::hash<db0::TagAddress>()(passive));
    }

    TEST( TagAddressTest , testLayoutMatchesUint64 )
    {
        ASSERT_EQ(sizeof(db0::TagAddress), sizeof(std::uint64_t));
        ASSERT_EQ(alignof(db0::TagAddress), alignof(db0::UniqueAddress));
    }

}
