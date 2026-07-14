// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/serialization/Types.hpp>
#include <dbzero/core/memory/Memspace.hpp>

using namespace std;

namespace tests

{

    class MemspaceTests: public MemspaceTestBase
    {
    };
    
    TEST_F( MemspaceTests , testMemspaceAllocatorCanAlloc )
    {
        // this test has been create to diagnose the crash on basic alloc
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        ASSERT_NO_THROW(memspace.alloc(100));
    }
    
    TEST_F( MemspaceTests , testMemspaceWideAllocationsArePageAligned )
    {
        // this test has been create to diagnose the crash on basic alloc
        auto memspace = m_workspace.getMemspace("my-test-prefix_1");
        for (unsigned int i = 0; i < 100; i++) {
            auto addr = memspace.alloc((rand() % 1000) + memspace.getPageSize() + 1);
            ASSERT_EQ(0, addr % memspace.getPageSize());
        }
    }

}
