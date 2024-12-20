#include <gtest/gtest.h>
#include <sstream>
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
            ASSERT_TRUE(getDiffs(bytes_1.data(), bytes_2.data(), bytes_1.size(), result));
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

}