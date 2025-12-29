// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/vspace/v_object.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <utils/TestBase.hpp>
#include <unordered_map>

using namespace db0;
using namespace std;

namespace tests 

{

    class VSpaceTests: public MemspaceTestBase
    {
    };

    TEST_F( VSpaceTests , testNewVObjectInstanceCanBeCreatedAndInitialized )
    {        
        auto memspace = getMemspace();
        db0::v_object<db0::o_simple<int> > cut(memspace, 123);

        ASSERT_EQ(cut->value(), 123);
    }
    
    TEST_F( VSpaceTests , testObjectInstanceCanBeRetrievedByAddress )
    {
        auto memspace = getMemspace();
        Address address = {};
        {
            db0::v_object<db0::o_simple<int> > cut(memspace, 123);
            address = cut.getAddress();
        }
        
        auto cut = db0::v_object<db0::o_simple<int> >(memspace.myPtr(address));
        ASSERT_EQ(cut->value(), 123);
    }
    
    TEST_F( VSpaceTests , testTwoObjectsWillBeAssignedDistinctAddresses )
    {
        auto memspace = getMemspace();

        db0::v_object<db0::o_simple<int> > i1(memspace, 123);
        db0::v_object<db0::o_simple<int> > i2(memspace, 456);
        ASSERT_NE(i1.getAddress(), i2.getAddress());
    }
    
    TEST_F( VSpaceTests , testMoveConstructorWorksWithVObjects )
    {        
        auto memspace = getMemspace();

        db0::v_object<db0::o_binary> i1(memspace, 4096);
        db0::v_object<db0::o_binary> i2(std::move(i1));        
        ASSERT_EQ(i2->size(), 4096);
    }
        
    TEST_F( VSpaceTests , testVObjectCanBeAccessedAfterDetach )
    {        
        auto memspace = getMemspace();

        db0::v_object<db0::o_binary> i1(memspace, 4096);
        i1.detach();
        // access v_object after detach
        ASSERT_EQ(i1->size(), 4096);
    }
    
    TEST_F( VSpaceTests , testMemLockPerformedOnceForObjectCreation )
    {        
        auto memspace = getMemspace();
        std::unordered_map<std::uint64_t, int> counts;
        m_workspace.setMapRangeCallback([&](std::uint64_t address, std::size_t size, FlagSet<AccessOptions> options) {
            if (counts.find(address) == counts.end()) {
                counts[address] = 0;
            }
            counts[address] += 1;
        });

        db0::v_object<db0::o_binary> i1(memspace, 4096);
        ASSERT_EQ(counts[i1.getAddress()], 1);
    }

}