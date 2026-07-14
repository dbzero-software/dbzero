// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <cstdlib>
#include <utility>

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <utils/utils.hpp>
#include <dbzero/core/serialization/packed_int.hpp>
#include <dbzero/core/serialization/packed_array.hpp>
    
using namespace db0;
using namespace db0::tests;

namespace tests

{
    
    class PackedArrayTest: public MemspaceTestBase
    {
    public:        
    };

    TEST_F( PackedArrayTest , testPackedArrayCreatedWithNoArgs )
    {        
        using T = o_packed_array<o_packed_int<std::uint16_t>, std::uint8_t, 64>;

        auto memspace = getMemspace();
        ASSERT_NO_THROW( (v_object<T>(memspace)) );
    }

    TEST_F( PackedArrayTest , testPackedArrayCanAppendPackedInts )
    {        
        using T = o_packed_array<o_packed_int<std::uint64_t>, std::uint8_t, 64>;

        auto memspace = getMemspace();
        v_object<T> cut(memspace);
        int count = 0;
        while (cut.modify().tryEmplaceBack(0)) {
            ++count;
        }
        ASSERT_EQ(count, (int)(64 - sizeof(std::uint8_t)));
    }

    TEST_F( PackedArrayTest , testPackedArrayIteration )
    {        
        using T = o_packed_array<o_packed_int<std::uint64_t>, std::uint8_t, 64>;

        auto memspace = getMemspace();
        v_object<T> cut(memspace);
        int count = 0;
        while (cut.modify().tryEmplaceBack(count)) {
            ++count;
        }

        count = 0;
        for (auto &item : *cut.getData()) {
            ASSERT_EQ(item, count);
            ++count;
        }
    }

}