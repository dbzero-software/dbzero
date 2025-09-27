#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <utils/TP_Utils.hpp>
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>
#include <dbzero/core/collections/full_text/TagProduct.hpp>
#include <dbzero/core/collections/full_text/TP_Vector.hpp>
#include <dbzero/core/collections/full_text/FT_ANDIterator.hpp>

namespace tests

{
    
	using namespace db0;
    using UniqueAddress = db0::UniqueAddress;

    class TagProductTest: public testing::Test
    {    
    protected:
        TP_Data m_data;
    };
    
    TEST_F( TagProductTest, testTP_IterateOverSingleSourceResults )
    {                
        std::vector<std::uint64_t> objects { 1, 2, 3 };
        std::vector<std::uint64_t> tags { 0, 1 };
        
        auto cut = makeTagProduct(objects, tags, m_data.m_index_1);
        unsigned int count = 0;
        TP_Vector<std::uint64_t> key;
        while (!cut.isEnd()) {
            cut.next(key);
            ++count;
        }
        ASSERT_EQ(count, 4);
    }
    
    /* FIXME:
    TEST_F( TagProductTest, testTP_JoinOperation )
    {
        std::vector<std::uint64_t> objects { 1, 2, 3 };
        std::vector<std::uint64_t> tags { 6, 9 };
        
        // join key / expected key
        std::vector<std::pair<TP_Key<std::uint64_t>, TP_Key<std::uint64_t>>> join_data {
            {{2, 15}, {2, 9}},
            {{2, 7}, {3, 6}},
            {{3, 6}, {3, 6}}
        };

        auto cut = makeTagProduct(objects, tags, m_data.m_index_2);
        for (auto &key: join_data) {
            ASSERT_FALSE(cut.isEnd());
            ASSERT_TRUE(cut.join(key.first));
            ASSERT_EQ(key.second, cut.getKey());
        }
        
        // iteration past bounds
        auto last_key = TP_Key<std::uint64_t>({1, 6});
        ASSERT_FALSE(cut.join(last_key));
    }
    */
    
}   