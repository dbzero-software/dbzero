// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

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
            cut.next(&key);
            ++count;
        }
        ASSERT_EQ(count, 4);
    }
    
    TEST_F( TagProductTest, testTP_MultipleObjectSources )
    {                
        std::vector<std::vector<std::uint64_t> > objects {
            { 1, 2, 3 }, { 101, 102, 103, 104 }
        };
        std::vector<std::uint64_t> tags { 0, 1 };
        
        auto cut = makeTagProduct(objects, tags, m_data.m_index_3);
        std::vector<std::vector<std::uint64_t> > expected_keys {
            { 3, 104 }, { 3, 103 },
            { 3, 103 }, { 2, 103 }, { 1, 103 }, 
            { 3, 102 }, { 2, 102 }, { 1, 102 },
            { 3, 101 }, { 2, 101 }, { 1, 101 }
        };
        
        for (auto &expected_key: expected_keys) {
            ASSERT_FALSE(cut.isEnd());
            TP_Vector<std::uint64_t> key;
            cut.next(&key);
            ASSERT_TRUE(key == expected_key.data());
        }
        ASSERT_TRUE(cut.isEnd());
    }
    
    TEST_F( TagProductTest, testTagProductBegin )
    {                
        std::vector<std::vector<std::uint64_t> > objects {
            { 1, 2, 3 }, { 101, 102, 103, 104 }
        };
        std::vector<std::uint64_t> tags { 0, 1 };
        
        auto cut = makeTagProduct(objects, tags, m_data.m_index_3);
        unsigned int count = 0;
        while (!cut.isEnd()) {            
            cut.next();
            ++count;
        }

        auto it = cut.begin();
        while (!it->isEnd()) {      
            it->next();
            --count;
        }
        ASSERT_EQ(count, 0);
    }

}   