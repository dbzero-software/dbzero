// SPDX-License-Identifier: AGPL-3.0-or-later
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