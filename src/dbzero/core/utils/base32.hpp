#pragma once

#include <cstdint>

namespace db0

{
    
    /**
     * Decode null-terminated string as base32, stops at a first non-base32 character
     * @param out output buffer of a sufficient size 
     * @return number of bytes written to the output buffer
    */
    std::size_t base32_decode(const char *buf, std::uint8_t *out) noexcept;

}