// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>
#include <dbzero/core/collections/full_text/FT_SpanIterator.hpp>

namespace tests

{

	using namespace db0;
    using UniqueAddress = db0::UniqueAddress;

    class FT_SpanIteratorTest : public testing::Test
    {
    };
    
    TEST_F( FT_SpanIteratorTest, testSpanIteratorOverPageNumbers )
    {                
        auto page_shift = 12;
        auto page_size = 1 << page_shift;
        // NOTE: pointing to the last element of the page due to direction = -1
        std::vector<std::uint64_t> dp_heads {
            (1u << page_shift) - 1, (2u << page_shift) - 1, (3u << page_shift) - 1, (4u << page_shift) - 1,
        };

        auto inner = std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            dp_heads.data(), dp_heads.data() + dp_heads.size()
        );

        FT_SpanIterator<std::uint64_t> cut(std::move(inner), page_shift);
        auto count = 0;
        while (!cut.isEnd()) {
            std::uint64_t key;
            cut.next(&key);
            ASSERT_EQ(key, ((dp_heads.size() - count - 1) << page_shift) + page_size - 1);
            ++count;
        }

        ASSERT_EQ(count, dp_heads.size());
    }

    TEST_F( FT_SpanIteratorTest, testSpanIteratorJoinResult )
    {        
        auto page_shift = 12;
        // NOTE: pointing to the last element of the page due to direction = -1
        std::vector<std::uint64_t> dp_heads {
            (2u << page_shift) - 1, (4u << page_shift) - 1, (7u << page_shift) - 1
        };
        
        auto inner = std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            dp_heads.data(), dp_heads.data() + dp_heads.size()
        );

        FT_SpanIterator<std::uint64_t> cut(std::move(inner), page_shift);

        // NOTE: join yields true for as long as the addres fits into the span
        std::vector<std::pair<std::uint64_t, bool> > results {
            { (8u << page_shift) + 14, true },
            { (8u << page_shift) + 13, true },
            { (7u << page_shift) + 1014, true }, 
            { (6u << page_shift) + 923, true },
            { (1u << page_shift) + 389, true },
            { (0u << page_shift) + 2094, false }
        };
        
        for (const auto &join_data: results) {
            ASSERT_EQ(join_data.second, cut.join(join_data.first)); 
        }
    }
    
    TEST_F( FT_SpanIteratorTest, testSpanIteratorJoinKeys )
    {                
        auto page_shift = 12;        
        std::vector<std::uint64_t> dp_heads {
            (2u << page_shift) - 1, (3u << page_shift) - 1, (5u << page_shift) - 1, (8u << page_shift) - 1
        };

        auto inner = std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            dp_heads.data(), dp_heads.data() + dp_heads.size()
        );

        FT_SpanIterator<std::uint64_t> cut(std::move(inner), page_shift);

        // NOTE: join yields true for as long as the addres fits into the span
        std::vector<std::pair<std::uint64_t, std::uint64_t> > results {
            { (8u << page_shift) + 14, (8u << page_shift) - 1 },
            { (8u << page_shift) + 13, (8u << page_shift) - 1 },
            { (7u << page_shift) + 1014, (7u << page_shift) + 1014 },
            { (6u << page_shift) + 923, (5u << page_shift) - 1 },
            { (1u << page_shift) + 389, (1u << page_shift) + 389 }         
        };
        
        for (const auto &join_data: results) {
            ASSERT_TRUE(cut.join(join_data.first));
            ASSERT_EQ(join_data.second, cut.getKey());
        }
    }
    
    TEST_F( FT_SpanIteratorTest, testSpanIteratorBeginTyped )
    {        
        auto page_shift = 12;
        // NOTE: pointing to the last element of the page due to direction = -1
        std::vector<std::uint64_t> dp_heads {
            (2u << page_shift) - 1, (4u << page_shift) - 1, (7u << page_shift) - 1
        };
        
        auto inner = std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            dp_heads.data(), dp_heads.data() + dp_heads.size()
        );

        FT_SpanIterator<std::uint64_t> cut(std::move(inner), page_shift);
        cut.join((1u << page_shift) + 389);
                
        std::vector<std::pair<std::uint64_t, bool> > results {
            { (8u << page_shift) + 14, true },
            { (8u << page_shift) + 13, true },
            { (7u << page_shift) + 1014, true }, 
            { (6u << page_shift) + 923, true },
            { (1u << page_shift) + 389, true },
            { (0u << page_shift) + 2094, false }
        };

        auto it = cut.beginTyped();
        for (const auto &join_data: results) {
            ASSERT_EQ(join_data.second, it->join(join_data.first)); 
        }        
    }
    
}