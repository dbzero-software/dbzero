// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <utils/TestBase.hpp>
#include <dbzero/core/collections/full_text/FT_FixedKeyIterator.hpp>
#include <dbzero/core/collections/full_text/CartesianProduct.hpp>
#include <dbzero/core/collections/full_text/FT_ANDIterator.hpp>

namespace tests

{

	using namespace db0;
    using UniqueAddress = db0::UniqueAddress;

    class CartesianProductTest : public testing::Test
    {
    public:
        std::unique_ptr<CartesianProduct<std::uint64_t> > 
        makeCartesianProduct(const std::vector<std::uint64_t> &keys, int direction)
        {
            std::vector<std::unique_ptr<FT_Iterator<std::uint64_t>>> components;
            components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
                keys.data(), keys.data() + keys.size()
            ));
            components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
                keys.data(), keys.data() + keys.size()
            ));
        
            return std::make_unique<CartesianProduct<std::uint64_t>>(std::move(components), direction);
        }
    };
    
    TEST_F( CartesianProductTest, testCP_IterateOverAllCombinations )
    {                
        std::vector<std::uint64_t> keys;
        for (std::uint64_t i = 0; i < 10; ++i) {
            keys.push_back(i);
        }

        // create the 2 identical components
        std::vector<std::unique_ptr<FT_Iterator<std::uint64_t>>> components;
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));

        CartesianProduct<std::uint64_t> cut(std::move(components), 1);
        auto count = 0;
        while (!cut.isEnd()) {
            ++cut;
            ++count;
        }

        ASSERT_EQ(count, keys.size() * keys.size());
    }

    TEST_F( CartesianProductTest, testCP_AccessComponentKeys )
    {                
        std::vector<std::uint64_t> keys;
        for (std::uint64_t i = 0; i < 10; ++i) {
            keys.push_back(i);
        }

        // create the 2 identical components
        std::vector<std::unique_ptr<FT_Iterator<std::uint64_t>>> components;
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));
        
        std::set<std::pair<std::uint64_t, std::uint64_t> > seen_keys;
        CartesianProduct<std::uint64_t> cut(std::move(components), 1);
        while (!cut.isEnd()) {
            seen_keys.insert(std::make_pair(cut.getKey()[0], cut.getKey()[1]));            
            ++cut;
        }

        ASSERT_EQ(seen_keys.size(), keys.size() * keys.size());
    }

    TEST_F( CartesianProductTest, testCP_ComponentOrder )
    {                
        std::vector<std::uint64_t> keys;
        for (std::uint64_t i = 0; i < 10; ++i) {
            keys.push_back(i);
        }

        // create the 2 identical components
        std::vector<std::unique_ptr<FT_Iterator<std::uint64_t>>> components;
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));
        
        // Low-order components change before high-order components
        CartesianProduct<std::uint64_t> cut(std::move(components), 1);
        std::uint64_t last_high = 0;
        while (!cut.isEnd()) {
            ASSERT_TRUE(!last_high || cut.getKey()[1] >= last_high);
            last_high = cut.getKey()[1];
            ++cut;
        }        
    }

    TEST_F( CartesianProductTest, testCP_JoinOperator )
    {
        std::vector<std::uint64_t> keys;
        for (std::uint64_t i = 0; i < 10; ++i) {
            keys.push_back(i);
        }

        // create the 2 identical components
        std::vector<std::unique_ptr<FT_Iterator<std::uint64_t>>> components;
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));
        
        // requested key, actual key
        std::vector<std::pair<std::uint64_t, std::uint64_t> > join_keys {
            {2, 3}, {2, 3},
            {11, 5}, {0, 6},
            {8, 8}, {8, 8},
            {0, 4}, {8, 8},
            {9, 9}, {9, 9}
        };
        
        CartesianProduct<std::uint64_t> cut(std::move(components), 1);
        auto key = join_keys.begin();
        auto expected_key = ++(join_keys.begin());
        std::array<std::uint64_t, 2> key_buf;
        for (; key != join_keys.end(); key += 2, expected_key += 2) {
            key_buf[0] = key->first;
            key_buf[1] = key->second;
            auto result = cut.join(key_buf.data(), 1);
            ASSERT_TRUE(result);
            ASSERT_EQ(cut.getKey()[0], expected_key->first);
            ASSERT_EQ(cut.getKey()[1], expected_key->second);
        }

        key_buf[0] = 9;
        key_buf[1] = 10;
        // out of bounds
        ASSERT_FALSE(cut.join(key_buf.data(), 1));
    }

    TEST_F( CartesianProductTest, testCP_JoinIssue )
    {
        std::vector<std::uint64_t> keys;
        for (std::uint64_t i = 0; i < 15; i += 2) {
            keys.push_back(i);
        }

        // create the 2 identical components
        std::vector<std::unique_ptr<FT_Iterator<std::uint64_t>>> components;
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));
        components.push_back(std::make_unique<FT_FixedKeyIterator<std::uint64_t>>(
            keys.data(), keys.data() + keys.size()
        ));
        
        // requested key, actual key
        std::vector<std::pair<std::uint64_t, std::uint64_t> > join_keys {
            {12, 0}, {12, 0},
            {0, 3}, {0, 4}
        };
        
        CartesianProduct<std::uint64_t> cut(std::move(components), 1);
        auto key = join_keys.begin();
        auto expected_key = ++(join_keys.begin());
        std::array<std::uint64_t, 2> key_buf;
        for (; key != join_keys.end(); key += 2, expected_key += 2) {
            key_buf[0] = key->first;
            key_buf[1] = key->second;
            auto result = cut.join(key_buf.data(), 1);
            ASSERT_TRUE(result);
            ASSERT_EQ(cut.getKey()[0], expected_key->first);
            ASSERT_EQ(cut.getKey()[1], expected_key->second);
        }
    }
    
    TEST_F( CartesianProductTest, testCP_ANDCombine )
    {
        std::vector<std::uint64_t> keys_1, keys_2;
        for (std::uint64_t i = 0; i < 15; i += 2) {
            keys_1.push_back(i);
        }
        for (std::uint64_t i = 0; i < 15; i += 3) {
            keys_2.push_back(i);
        }

        // create 2 different cartesian products
        auto cp1 = makeCartesianProduct(keys_1, 1);
        auto cp2 = makeCartesianProduct(keys_2, 1);
        
        FT_ANDIteratorFactory<const std::uint64_t*, true, CP_Vector<std::uint64_t> > factory;
        factory.add(std::move(cp1));
        factory.add(std::move(cp2));
        auto it = factory.release(1);
        unsigned int count = 0;
        while (!it->isEnd()) {
            auto key = it->getKey();            
            ASSERT_EQ(key[0] % 6, 0);
            ASSERT_EQ(key[1] % 6, 0);
            ++(*it);
            ++count;
        }
        ASSERT_EQ(count, 9);
    }
    
    TEST_F( CartesianProductTest, testCP_ORXCombine )
    {
        std::vector<std::uint64_t> keys_1, keys_2;
        for (std::uint64_t i = 0; i < 15; i += 3) {
            keys_1.push_back(i);
        }
        for (std::uint64_t i = 0; i < 15; i += 5) {
            keys_2.push_back(i);
        }

        // create 2 different cartesian products
        auto cp1 = makeCartesianProduct(keys_1, 1);
        auto cp2 = makeCartesianProduct(keys_2, 1);
        
        FT_ORXIteratorFactory<const std::uint64_t*, CP_Vector<std::uint64_t> > factory;
        factory.add(std::move(cp1));
        factory.add(std::move(cp2));
        auto it = factory.release(1);
        unsigned int count = 0;
        while (!it->isEnd()) {
            auto key = it->getKey();
            ASSERT_TRUE((key[0] % 3 == 0 || key[0] % 5 == 0));
            ASSERT_TRUE((key[1] % 3 == 0 || key[1] % 5 == 0));
            ++(*it);
            ++count;
        }
        ASSERT_EQ(count, 33);
    }
    
}   