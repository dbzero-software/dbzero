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
            auto rand_size = rand() % 100;
            std::vector<std::uint8_t> data(rand_size);
            for (int j = 0; j < rand_size; ++j) {
                data[j] = rand() % 256;
            }            
            db0::base32_encode(data.data(), data.size(), buf);
            // decoded might be up to 1 byte larger than original
            std::vector<std::uint8_t> decoded(rand_size + 1);
            auto decoded_size = db0::base32_decode(buf, decoded.data());
            ASSERT_TRUE(abs(static_cast<long long int>(decoded_size - rand_size)) <= 1);
            // compare encoded / decoded bytes
            for (int j = 0; j < rand_size; ++j) {
                ASSERT_EQ(data[j], decoded[j]);
            }
        }
    }

}
