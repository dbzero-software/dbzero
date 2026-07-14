// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>

namespace tests

{

	using namespace db0;    

    class FT_FixedKeyIteratorTest : public testing::Test
    {
    public:
        std::unique_ptr<FT_FixedKeyIterator<std::uint64_t> > makeIterator(const std::vector<std::uint64_t> &keys, 
            int direction = -1)
        {
            return std::unique_ptr<FT_FixedKeyIterator<std::uint64_t> >(new FT_FixedKeyIterator<std::uint64_t>(
                keys.data(), keys.data() + keys.size(), direction
            ));
        } 
    };
    
    TEST_F( FT_FixedKeyIteratorTest, testFT_FixedKeyForwardIterator )
    {                
        std::vector<std::uint64_t> keys;
        for (std::uint64_t i = 0; i < 10; ++i) {
            keys.push_back(i);
        }
                
        FT_FixedKeyIterator<std::uint64_t> cut(keys.data(), keys.data() + keys.size(), -1);
        // create forward iterator from backward
        auto it = cut.beginTyped(1);
        
        std::uint64_t last_key = 0;
        while (!(*it).isEnd()) {
            ASSERT_TRUE(!last_key || last_key < (*it).getKey());
            last_key = (*it).getKey();
            ++(*it);
        }
    }

    TEST_F( FT_FixedKeyIteratorTest, testANDJoinFixedKeysIssue1 )
    {                
        std::vector<std::uint64_t> keys_0 { 0, 1, 2, 3 };
        std::vector<std::uint64_t> keys_1 { 1, 2, 3 };
        
        auto cut = std::make_unique<FT_JoinANDIterator<std::uint64_t> >(
            makeIterator(keys_0), makeIterator(keys_1), -1
        );

        unsigned int count = 0;
        while (!cut->isEnd()) {
            (*cut).next();
            ++count;
        }
        ASSERT_EQ(count, 3u);
    }

}