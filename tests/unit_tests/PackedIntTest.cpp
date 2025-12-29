// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <sstream>

#include <dbzero/core/serialization/packed_int.hpp>

using namespace std;
using namespace db0;

namespace tests 

{

    class PackedIntTest : public testing::Test 
    {
    };

    TEST_F( PackedIntTest, testCanProperlyEncodeDecodeNullablePackedInt )
    {
        using T = db0::o_packed_int<std::uint32_t, true>;
        std::vector<std::uint32_t> values { 127, 0, 1, 63, 64, 127, 128, 255, 256, 187123, 9372724, 3837482273 };
        std::vector<std::size_t> expected_size { 2, 1, 1, 1, 1, 2, 2, 2, 2, 3, 4, 5 };
        auto size_of = expected_size.begin();
        for (auto value: values) {
            ASSERT_EQ(*size_of, T::measure(value));
            std::vector<char> buf(*size_of);
            auto &packed_value = T::__new(buf.data(), value);
            ASSERT_EQ(value, packed_value.value());
            ASSERT_EQ(buf.size(), packed_value.sizeOf());
            ASSERT_FALSE(packed_value.isNull());
            ++size_of;
        }
    }

    TEST_F( PackedIntTest, testCanProperlyEncodeNullValue ) 
    {
        using T = db0::o_packed_int<std::uint32_t, true>;
        // expected size for null element is 1
        ASSERT_EQ(1u, T::measure());
        std::vector<char> buf(1u);
        auto &packed_value = T::__new(buf.data());
        ASSERT_TRUE(packed_value.isNull());
    }

    TEST_F( PackedIntTest, testThrowsOnAttemptToDecodeNullValue )
    {
        using T = db0::o_packed_int<std::uint32_t, true>;
        std::vector<char> buf(T::measure());
        auto &packed_value = T::__new(buf.data());
        ASSERT_ANY_THROW(packed_value.value());
    }
    
    TEST_F( PackedIntTest, testPackedIntMeasure )
    {
        ASSERT_EQ(1u, db0::packed_int32::measure(2u));
    }

}