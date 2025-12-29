// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/memory/Address.hpp>
#include <dbzero/core/metaprog/binary_cast.hpp>

namespace tests

{
    
    TEST( UniqueAddressTest , testUniqueAddressCanCastToAddress )
    {
        auto addr = db0::Address::fromOffset(0x12345678);
        db0::UniqueAddress unique_addr(addr, 1);
        ASSERT_EQ(addr, unique_addr);
    }

    TEST( UniqueAddressTest , testUniqueAddressCanGetInstanceId )
    {        
        db0::UniqueAddress unique_addr(db0::Address::fromOffset(0x12345678), 1234);
        ASSERT_EQ(unique_addr.getInstanceId(), 1234);
    }

    TEST( UniqueAddressTest , testUniqueAddressToValueConversion )
    {
        db0::UniqueAddress unique_addr(db0::Address::fromOffset(0x12345678), 1234);
        auto value = unique_addr.getValue();
        ASSERT_EQ(db0::UniqueAddress::fromValue(value), unique_addr);
    }
    
    TEST( UniqueAddressTest , testUniqueAddressBinaryCastToUInt64 )
    {
        auto cast = db0::binary_cast<db0::UniqueAddress, std::uint64_t>();
        db0::UniqueAddress unique_addr(db0::Address::fromOffset(0x12345678), 1234);
        // cast to and from uint64_t
        auto value = cast(unique_addr);
        ASSERT_EQ(unique_addr, cast(value));
    }
    
}