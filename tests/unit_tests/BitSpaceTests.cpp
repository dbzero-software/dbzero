// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/memory/BitSpace.hpp>
#include <dbzero/core/serialization/Types.hpp>

using namespace std;

namespace tests

{

    class BitSpaceTests: public MemspaceTestBase
    {    
    };
    
    TEST_F( BitSpaceTests , testPageSizeVObjectsCanBeCreatedOnBitSpace )
    {        
        auto memspace = getMemspace();

        // initialize the bitspace
        std::size_t page_size = 4096;
        auto base_addr = db0::Address::fromOffset(0);
        // configure bitspace to use the entire 4kb page - i.e. 0x8000 bits
        db0::BitSpace<0x8000> bitspace(memspace.getPrefixPtr(), base_addr, page_size);
        
        using ObjectT = db0::v_object<db0::o_binary>;
        ObjectT obj1(bitspace, page_size - db0::o_binary::sizeOfFixedPart());
        ObjectT obj2(bitspace, page_size - db0::o_binary::sizeOfFixedPart());

        // make sure objects were allocated under different addresses
        ASSERT_NE(obj1.getAddress(), obj2.getAddress());
    }

}
