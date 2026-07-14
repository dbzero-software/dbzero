// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/utils/FlagSet.hpp>
#include <dbzero/core/memory/AccessOptions.hpp>

using namespace std;
using namespace db0;

namespace tests

{

    enum class TestOptions : std::uint32_t {
        first = 0x01,
        second = 0x02,
        third = 0x04,
        fourth = 0x08,
        fifth = 0x10
    };

    enum class UndeclaredTestOptions : std::uint32_t {
        first = 0x01,
        second = 0x02,
        third = 0x04,
        fourth = 0x08,
        fifth = 0x10
    };

}

DECLARE_ENUM_VALUES(tests::TestOptions, 5)
DEFINE_ENUM_VALUES(tests::TestOptions, "first", "second", "third", "fourth", "fifth")

namespace tests 

{

    class FlagSetTest : public testing::Test {
    public :
    };

    TEST_F( FlagSetTest, testFlagSetCanUnpackFlags ) 
    {
        using Cut = FlagSet<TestOptions>;
        Cut cut { TestOptions::first, TestOptions::third, TestOptions::fourth };
        std::vector<TestOptions> expected_result { TestOptions::first, TestOptions::third, TestOptions::fourth };
        ASSERT_EQ (expected_result, cut.unpack());
    }

    TEST_F( FlagSetTest, testFlagSetCanUnpackFlagsToStringVector ) 
    {
        using Cut = FlagSet<TestOptions>;
        Cut cut { TestOptions::third, TestOptions::fifth, TestOptions::second };
        std::vector<std::string> expected_result { "second", "third", "fifth" };
        ASSERT_EQ (expected_result, cut.unpack<std::string>());
    }

    TEST_F( FlagSetTest, testFlagSetCanBeInitializedFromStringVector ) 
    {
        using Cut = FlagSet<TestOptions>;
        std::vector<std::string> str_flags { "second", "third", "fifth" };
        Cut cut(str_flags);
        std::vector<TestOptions> expected_result { TestOptions::second, TestOptions::third, TestOptions::fifth };
        ASSERT_EQ (expected_result, cut.unpack());
    }

    TEST_F( FlagSetTest, testFlagSetAllCanBeUsedToFechAllFlags ) 
    {
        auto flags = FlagSet<TestOptions>::all();
        ASSERT_EQ (0x1f, flags.value());
    }

    TEST_F( FlagSetTest, testFlagSetAllThrowsWhenUndeclaredOptions ) 
    {
        ASSERT_ANY_THROW ( FlagSet<UndeclaredTestOptions>::all() );
    }
    
    TEST_F( FlagSetTest, testFlagSetCanBeOutputToStream )
    {
        auto cut = FlagSet<TestOptions>::all();
        std::stringstream ss;
        ss << cut;
        ASSERT_EQ ("[first, second, third, fourth, fifth]", ss.str());
    }

    TEST_F( FlagSetTest, testAccessOptionsCanBeOutputToStream )
    {
        auto cut = FlagSet<AccessOptions>::all();
        std::stringstream ss;
        ss << cut;
        ASSERT_EQ ("[read, write, create, no_cache, no_flush, unique, no_cow]", ss.str());
    }

}
