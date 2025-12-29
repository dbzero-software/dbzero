// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cstddef>

namespace db0

{
    
    /**
     * Decode null-terminated string as base32, stops at a first non-base32 character
     * @param out output buffer of a sufficient size 
     * @return number of bytes written to the output buffer
    */
    std::size_t base32_decode(const char *buf, std::uint8_t *out) noexcept;
    
    /**
     * Encode bytes as base-32
     * @param in input buffer
     * @param size number of bytes to encode
     * @param out output buffer of a sufficient size
     * @return number of characters written to the output buffer
    */
    std::size_t base32_encode(const std::uint8_t *in, std::size_t size, char *out) noexcept;
    
}