#include <gtest/gtest.h>
#include <sstream>
#include <vector>
#include <cstdint>
#include <cstring>
#include <dbzero/core/memory/ResourceLock.hpp>

using namespace std;
using namespace db0;

namespace tests 

{

    class ResourceLockTest : public testing::Test
    {
    };
    
    TEST_F( ResourceLockTest, testCalculateDPDiff )
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
    
    TEST_F( ResourceLockTest, testCalculateDPDiffWhenMaxDiffExceeded )
    {
        std::vector<std::uint8_t> bytes_1 = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        std::vector<std::uint8_t> bytes_2 = { 1, 2, 3, 4, 4, 5, 7, 7, 7, 7 };
        
        std::vector<std::uint16_t> result;
        ASSERT_FALSE(getDiffs(bytes_1.data(), bytes_2.data(), bytes_1.size(), result));
    }
    
    TEST_F( ResourceLockTest , testDPDiffDoesNotContainTrailingZeros )
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

}