// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <sstream>
#include <cstdint>
#include <vector>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/core/serialization/packed_int_pair.hpp>

using namespace std;
using namespace db0;

namespace tests 

{
    
    class PackeIntPairTest : public testing::Test
    {
    };

    TEST_F( PackeIntPairTest, testValueOfPackedIntPair )
    {
        using T = db0::o_packed_int_pair<std::uint32_t, std::uint64_t>;
        std::vector<std::byte> data(128);
        auto &cut = T::__new(data.data(), 123, 24342);
        ASSERT_EQ(cut.sizeOf(), (o_packed_int<std::uint32_t, false>::measure(123) + o_packed_int<std::uint64_t, false>::measure(24342)));
        ASSERT_EQ(cut.value(), (std::pair<std::uint32_t, std::uint64_t>(123, 24342)));
    }

}