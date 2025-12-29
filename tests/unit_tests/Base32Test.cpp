// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include <gtest/gtest.h>
#include <dbzero/core/utils/base32.hpp>
#include <cstdlib>

namespace tests

{
    
    TEST( Base32Test, testBase32Encode )
    {
        char buf[1024];
        for (unsigned int i = 0; i < 100; ++i) {
            auto rand_size = rand() % 100;
            std::vector<std::uint8_t> data(rand_size);
            for (int j = 0; j < rand_size; ++j) {
                data[j] = rand() % 256;
            }
            db0::base32_encode(data.data(), data.size(), buf);
            ASSERT_EQ(strlen(buf), rand_size * 8 / 5 + (rand_size % 5 ? 1 : 0));
        } 
    }

    TEST( Base32Test, testBase32Decode )
    {
        char buf[1024];
        for (unsigned int i = 0; i < 100; ++i) {
            unsigned int rand_size = rand() % 100;
            std::vector<std::uint8_t> data(rand_size);
            for (unsigned int j = 0; j < rand_size; ++j) {
                data[j] = rand() % 256;
            }            
            db0::base32_encode(data.data(), data.size(), buf);
            // decoded might be up to 1 byte larger than original
            std::vector<std::uint8_t> decoded(rand_size + 1);
            auto decoded_size = db0::base32_decode(buf, decoded.data());
            ASSERT_TRUE(decoded_size >= rand_size && decoded_size <= rand_size + 1);            
            // compare encoded / decoded bytes
            for (unsigned int j = 0; j < rand_size; ++j) {
                ASSERT_EQ(data[j], decoded[j]);
            }
        }
    }
    
    TEST( Base32Test, testBase32Decode20 )
    {
        char buf[1024];        
        std::vector<std::uint8_t> data(20);
        for (int j = 0; j < 20; ++j) {
            data[j] = rand() % 256;
        }            
        db0::base32_encode(data.data(), data.size(), buf);
        // decode requires +1 byte
        std::vector<std::uint8_t> decoded(20 + 1);
        ASSERT_EQ(db0::base32_decode(buf, decoded.data()), 20);
    }

}
