// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/collections/bitset/FixedBitset.hpp>
#include <utils/TestBase.hpp>

using namespace std;

namespace tests 

{

    class FixedBitsetTests: public MemspaceTestBase
    {
    };
    
    TEST_F( FixedBitsetTests , testNewFixedBitsetCreatetAsEmpty )
    {        
        auto memspace = getMemspace();

        db0::VFixedBitset<123> cut(memspace);
        for (unsigned int i = 0; i < 123; ++i) {
            ASSERT_FALSE(cut->get(i));
        }
    }

    TEST_F( FixedBitsetTests , testFixedBitsetSetAndGet )
    {        
        auto memspace = getMemspace();

        db0::VFixedBitset<123> cut(memspace);
        std::vector<unsigned int> setBits = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 122 };
        for (auto i : setBits) {
            cut.modify().set(i, true);
        }
        for (unsigned int i = 0; i < 123; ++i) {
            if (std::find(setBits.begin(), setBits.end(), i) != setBits.end()) {
                ASSERT_TRUE(cut->get(i));
            } else {
                ASSERT_FALSE(cut->get(i));
            }
        }
    }

    TEST_F( FixedBitsetTests , testFixedBitsetFirstIndexOf )
    {        
        auto memspace = getMemspace();

        db0::VFixedBitset<123> cut(memspace);
        ASSERT_EQ(cut->firstIndexOf(false), 0);
        std::vector<unsigned int> setBits = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 121, 122 };
        for (auto i : setBits) {
            cut.modify().set(122 - i, true);
            ASSERT_EQ(cut->firstIndexOf(true), 122 - i);
        }

        ASSERT_EQ(cut->firstIndexOf(false), 2);
    }
    
    TEST_F( FixedBitsetTests , testFixedBitsetLastIndexOf )
    {
        auto memspace = getMemspace();

        db0::VFixedBitset<123> cut(memspace);
        ASSERT_EQ(cut->lastIndexOf(false), 122);
        ASSERT_EQ(cut->lastIndexOf(true), -1);
        std::vector<unsigned int> setBits = { 0, 1, 2, 3, 4, 5, 6, 28, 55, 99, 121, 122 };
        for (auto i : setBits) {
            cut.modify().set(i, true);
            ASSERT_EQ(cut->lastIndexOf(true), i);
        }
    }

}