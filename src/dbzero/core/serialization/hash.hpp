#pragma once

#include <cstdint>
#include <vector>

namespace db0::serial

{
    
    void sha256(const std::uint8_t* message, std::uint64_t len, std::uint32_t hash[8]);
        
}