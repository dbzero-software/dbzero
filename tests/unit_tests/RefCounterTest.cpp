// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <sstream>

#include <dbzero/core/serialization/ref_counter.hpp>

using namespace std;
using namespace db0;

namespace tests 

{

    class RefCounterTest : public testing::Test
    {
    };

    TEST_F( RefCounterTest, testRefCounterDefaultInitialization )
    {
        std::vector<char> buf(8);
        using RefCounterT = db0::o_ref_counter<std::uint32_t, 4>;
        auto &cut = RefCounterT::__new(buf.data());
        ASSERT_EQ(0u, cut.getFirst());
        ASSERT_EQ(0u, cut.getSecond());
        ASSERT_EQ((std::pair<std::uint32_t, std::uint32_t>(0, 0)), cut.get());
    }
    
    TEST_F( RefCounterTest, testRefCounterInitializationWithValues )
    {
        std::vector<char> buf(8);
        using RefCounterT = db0::o_ref_counter<std::uint32_t, 6>;
        auto &cut = RefCounterT::__new(buf.data(), 1234567, 890);
        ASSERT_EQ(1234567u, cut.getFirst());
        ASSERT_EQ(890u, cut.getSecond());
        ASSERT_EQ((std::pair<std::uint32_t, std::uint32_t>(1234567, 890)), cut.get());
    }

    TEST_F( RefCounterTest, testRefCounterSetFirst )
    {
        std::vector<char> buf(8);
        using RefCounterT = db0::o_ref_counter<std::uint32_t, 6>;
        auto &cut = RefCounterT::__new(buf.data(), 1234567, 890);
        cut.setFirst(123);
        // set shrinkg size
        ASSERT_EQ(123u, cut.getFirst());
        ASSERT_EQ(890u, cut.getSecond());
        // set larger size
        cut.setFirst(123456789);
        ASSERT_EQ(123456789u, cut.getFirst());
        ASSERT_EQ(890u, cut.getSecond());
    }

    TEST_F( RefCounterTest, testRefCounterSetSecond )
    {
        std::vector<char> buf(8);
        using RefCounterT = db0::o_ref_counter<std::uint32_t, 6>;
        auto &cut = RefCounterT::__new(buf.data(), 890, 1234567);
        // set shrinkg size
        cut.setSecond(123);        
        ASSERT_EQ(890u, cut.getFirst());
        ASSERT_EQ(123u, cut.getSecond());
        // set larger size
        cut.setSecond(123456789);
        ASSERT_EQ(890u, cut.getFirst());
        ASSERT_EQ(123456789u, cut.getSecond());
    }

    TEST_F( RefCounterTest, testRefCounterOverflowOnInit )
    {
        std::vector<char> buf(8);
        using RefCounterT = db0::o_ref_counter<std::uint32_t, 4>;
        ASSERT_ANY_THROW({
            RefCounterT::__new(buf.data(), 123456789, 890);
        });
    }

    TEST_F( RefCounterTest, testRefCounterOverflowOnSetFirst )
    {
        std::vector<char> buf(8);
        using RefCounterT = db0::o_ref_counter<std::uint32_t, 4>;
        auto &cut = RefCounterT::__new(buf.data(), 12345, 890);
        ASSERT_ANY_THROW({
            cut.setFirst(123456789);
        });
    }
    
    TEST_F( RefCounterTest, testRefCounterOverflowOnSetSecond )
    {
        std::vector<char> buf(8);
        using RefCounterT = db0::o_ref_counter<std::uint32_t, 4>;
        auto &cut = RefCounterT::__new(buf.data(), 12345, 890);
        ASSERT_ANY_THROW({
            cut.setSecond(123456789);
        });
    }

}