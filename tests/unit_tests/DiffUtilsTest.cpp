// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <sstream>
#include <vector>
#include <cstdint>
#include <cstring>
#include <dbzero/core/memory/diff_utils.hpp>

using namespace std;
using namespace db0;

namespace tests 

{

    class DiffUtilsTest : public testing::Test
    {
    };
    
    TEST_F( DiffUtilsTest, testCalculateDPDiff )
    {
        std::vector<std::uint8_t> bytes_1 = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        std::vector<std::uint8_t> bytes_2 = { 0, 1, 2, 4, 4, 5, 7, 7, 7, 7 };
        std::vector<std::uint8_t> bytes_3 = { 1, 2, 3, 4, 4, 5, 7, 7, 7, 7 };
        
        {
            std::vector<std::uint16_t> result;
            ASSERT_TRUE(getDiffs(bytes_1.data(), bytes_2.data(), bytes_1.size(), result, 128));
            ASSERT_EQ(result, (std::vector<std::uint16_t> { 0, 3, 1, 2, 1, 1, 2 }));
        }

        {
            std::vector<std::uint16_t> result;
            ASSERT_TRUE(getDiffs(bytes_1.data(), bytes_3.data(), bytes_1.size(), result, 128));
            ASSERT_EQ(result, (std::vector<std::uint16_t> { 4, 2, 1, 1, 2}));
        }    
    }
    
    TEST_F( DiffUtilsTest, testCalculateDPDiffWithRanges )
    {
        std::vector<std::uint8_t> bytes_1 = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        std::vector<std::uint8_t> bytes_2 = { 0, 1, 2, 4, 4, 5, 7, 7, 7, 7 };
        std::vector<std::uint8_t> bytes_3 = { 1, 2, 3, 4, 4, 5, 7, 7, 7, 7 };
        DiffRange diff_ranges (std::vector<std::pair<std::uint16_t, std::uint16_t>> {
            { 1, 2 }, { 1, 2 }, { 7, 10 } 
        } );
        
        {
            std::vector<std::uint16_t> result;
            ASSERT_TRUE(getDiffs(bytes_1.data(), bytes_2.data(), bytes_1.size(), result, 128, {}, diff_ranges));
            ASSERT_EQ(result, (std::vector<std::uint16_t> { 0, 1, 1, 1, 1, 2, 4 }));
        }

        {
            std::vector<std::uint16_t> result;
            ASSERT_TRUE(getDiffs(bytes_1.data(), bytes_3.data(), bytes_1.size(), result, 128, {}, diff_ranges));
            ASSERT_EQ(result, (std::vector<std::uint16_t> { 4, 2, 4 }));
        }    
    }

    TEST_F( DiffUtilsTest, testCalculateDPDiffWhenMaxDiffExceeded )
    {
        std::vector<std::uint8_t> bytes_1 = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        std::vector<std::uint8_t> bytes_2 = { 1, 2, 3, 4, 4, 5, 7, 7, 7, 7 };
        
        std::vector<std::uint16_t> result;
        ASSERT_FALSE(getDiffs(bytes_1.data(), bytes_2.data(), bytes_1.size(), result));
    }
    
    TEST_F( DiffUtilsTest , testDPDiffDoesNotContainTrailingZeros )
    {
        std::size_t page_size = 4096;
        std::vector<std::byte> dp_0(page_size, std::byte(0));
        std::vector<std::byte> dp_1(page_size, std::byte(0));
        std::memset(dp_1.data() + 18, 1, 32);
        std::memset(dp_1.data() + 94, 2, 11);
        std::memset(dp_1.data() + 1200, 3, 120);

        std::vector<std::uint16_t> diff_buf;
        db0::getDiffs(dp_0.data(), dp_1.data(), page_size, diff_buf);
        ASSERT_NE(diff_buf.back(), 0);
    }
    
    TEST_F( DiffUtilsTest, testDPDiffZeroFillIndicator )
    {
        std::vector<std::uint8_t> bytes_1 = { 0, 0, 1, 3, 4, 0, 0, 0, 0, 9 };
        std::vector<std::uint16_t> result;
        ASSERT_TRUE(getDiffs(bytes_1.data(), bytes_1.size(), result, 128));
        // make sure the zero-fill indicator is present
        ASSERT_EQ(result[0], 0);
        ASSERT_EQ(result[1], 0);
    }
    
    TEST_F( DiffUtilsTest, testCalculateDPZeroDiff )
    {
        std::vector<std::uint8_t> bytes_1 = { 0, 0, 1, 3, 4, 0, 0, 0, 0, 9 };
        std::vector<std::uint8_t> bytes_2 = { 0, 1, 2, 4, 4, 0, 7, 0, 7, 0 };
        
        {
            std::vector<std::uint16_t> result;
            ASSERT_TRUE(getDiffs(bytes_1.data(), bytes_1.size(), result, 128));
            ASSERT_EQ(result, (std::vector<std::uint16_t> { 0, 0, 0, 2, 3, 4, 1 }));
        }
        
        {
            std::vector<std::uint16_t> result;
            ASSERT_TRUE(getDiffs(bytes_2.data(), bytes_2.size(), result, 128));
            ASSERT_EQ(result, (std::vector<std::uint16_t> { 0, 0, 0, 1, 4, 1, 1, 1, 1 }));
        }
    }

    TEST_F( DiffUtilsTest, testCalculateDPZeroDiffWithRanges )
    {
        std::vector<std::uint8_t> bytes_1 = { 0, 0, 1, 3, 4, 0, 0, 0, 0, 9 };
        std::vector<std::uint8_t> bytes_2 = { 0, 1, 2, 4, 4, 0, 7, 0, 7, 0 };
        DiffRange diff_ranges(std::vector<std::pair<std::uint16_t, std::uint16_t> > {
            { 0, 2 }, { 6, 9 } }
        );        

        {
            std::vector<std::uint16_t> result;
            ASSERT_TRUE(getDiffs(bytes_1.data(), bytes_1.size(), result, 128, {}, diff_ranges));
            ASSERT_EQ(result, (std::vector<std::uint16_t> { 0, 0, 5, 1, 4 }));
        }
        
        {
            std::vector<std::uint16_t> result;
            ASSERT_TRUE(getDiffs(bytes_2.data(), bytes_2.size(), result, 128, {}, diff_ranges));
            ASSERT_EQ(result, (std::vector<std::uint16_t> { 0, 0, 5, 1, 3 }));
        }
    }
    
    TEST_F( DiffUtilsTest, testGetDiffsOfIdentities )
    {
        std::vector<std::uint8_t> bytes_1 = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        // all-zero buffer
        std::vector<std::uint8_t> bytes_2(100, 0);

        std::vector<std::uint16_t> result { 1, 2, 3 };
        ASSERT_TRUE(getDiffs(bytes_1.data(), bytes_1.data(), bytes_1.size(), result));
        ASSERT_TRUE(result.empty());
        
        result = { 1, 2, 3 };
        // zero-base diff cannot be empty because of the case when the writer fills entire DP with zeros
        ASSERT_TRUE(getDiffs(bytes_2.data(), bytes_2.size(), result));
        ASSERT_EQ(result[0], 0);
        ASSERT_EQ(result[1], 0);
    }

    TEST_F( DiffUtilsTest, testPrepareDiffRanges )
    {
        std::vector<std::pair<std::uint16_t, std::uint16_t>> diff_ranges_1 = {
            { 0, 1 }, { 2, 4 }, { 3, 8 }, { 0, 1 }
        };
        std::vector<std::pair<std::uint16_t, std::uint16_t>> expected_1 = {
            { 0, 1 }, { 2, 8 }
        };
        
        ASSERT_EQ(DiffRange(diff_ranges_1).getData(), expected_1);

        std::vector<std::pair<std::uint16_t, std::uint16_t>> diff_ranges_2 = {
            { 150, 190 }, { 120, 180 }, { 0, 1 }, { 0, 1}, { 0, 1 }, { 1, 13 }, { 12, 20 }, { 0, 1 }, { 33, 50 }
        };
        std::vector<std::pair<std::uint16_t, std::uint16_t>> expected_2 = {
            { 0, 20 }, { 33, 50 }, { 120, 190 }
        };
        
        ASSERT_EQ(DiffRange(diff_ranges_2).getData(), expected_2);
    }

    TEST_F( DiffUtilsTest, testDiffRangeView )
    {
        DiffRange diff_ranges(std::vector<std::pair<std::uint16_t, std::uint16_t>> {
            { 0, 100 }, { 4090, 4200 }, { 900, 1024 }, { 2000, 3000 }, { 5111, 9000 } }
        );

        auto get_data = [&](const DiffRangeView &view) {
            std::vector<std::pair<std::uint16_t, std::uint16_t>> result;
            for (std::size_t i = 0; i < view.size(); ++i) {
                result.push_back(view[i]);
            }
            return result;
        };
        
        DiffRangeView view(diff_ranges, 0, 100);
        ASSERT_EQ(get_data(view), (std::vector<std::pair<std::uint16_t, std::uint16_t>> { { 0, 100 } }));

        view = DiffRangeView(diff_ranges, 0, 2000);
        ASSERT_EQ((get_data(view)), (std::vector<std::pair<std::uint16_t, std::uint16_t>> { 
            { 0, 100 }, { 900, 1024 } })
        );
        
        view = DiffRangeView(diff_ranges, 0, 4090);
        ASSERT_EQ((get_data(view)), (std::vector<std::pair<std::uint16_t, std::uint16_t>> { 
            { 0, 100 }, { 900, 1024 }, { 2000, 3000 } })
        );

        view = DiffRangeView(diff_ranges, 0, 5200);
        ASSERT_EQ((get_data(view)), (std::vector<std::pair<std::uint16_t, std::uint16_t>> { 
            { 0, 100 }, { 900, 1024 }, { 2000, 3000 }, { 4090, 4200 }, { 5111, 5200 } })
        );

        view = DiffRangeView(diff_ranges, 4096, 8192);
        ASSERT_EQ((get_data(view)), (std::vector<std::pair<std::uint16_t, std::uint16_t>> {
            { 4096 - 4096, 4200 - 4096 }, { 5111 - 4096, 8192 - 4096 } })
        );        
    }
    
}