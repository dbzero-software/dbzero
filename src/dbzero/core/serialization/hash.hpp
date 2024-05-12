#pragma once

#include <cstdint>
#include <vector>

namespace db0::serial

{
    
    // @param hash the 32 character/byte array to store the hash
    void sha256(const std::uint8_t* message, std::uint64_t len, std::byte *hash);

    // Compute hash and append to the output vector
    void sha256(const std::vector<std::byte> &message, std::vector<std::byte> &output);

}