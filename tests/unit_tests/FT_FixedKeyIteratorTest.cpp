#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>

namespace tests

{

	using namespace db0;    

    class FT_FixedKeyIteratorTest : public testing::Test
    {
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

}