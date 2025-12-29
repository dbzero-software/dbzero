// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/workspace/PrefixName.hpp>

using namespace std;
using namespace db0;

namespace tests 

{

    class PrefixNameTest : public testing::Test
    {
    };
    
    TEST_F( PrefixNameTest, testPrefixNameTrimLeadingCharacters )
    {
        {
            auto cut = PrefixName("/test/prefix");
            ASSERT_EQ(cut.get(), "test/prefix");
        }
        {
            auto cut = PrefixName("\\test\\prefix");
            ASSERT_EQ(cut.get(), "test\\prefix");
        }
    }

}