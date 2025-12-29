// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/collections/b_index/IttyIndex.hpp>
#include <set>
#include <utils/TestBase.hpp>
#include <dbzero/core/memory/Address.hpp>

namespace tests 

{

    using Address = db0::Address;
    
    class IttyIndexTest: public MemspaceTestBase
    {
    public:
    };
    
    TEST_F( IttyIndexTest , testIttyIndexActsAsVSpaceDataStructure ) 
    {
        auto memspace = getMemspace();

        // 1. create new itty_index / store 1 value
        Address ptr;
        {
            db0::IttyIndex<std::uint64_t, Address> index(memspace, 12345);
            ptr = index.getAddress();
        }
        // 2. open existing
        {
            db0::IttyIndex<std::uint64_t, Address> index(std::make_pair(&memspace, ptr));
            // make sure this has proper value inside
            ASSERT_EQ(12345, index.getValue());
        }
    }

} 
