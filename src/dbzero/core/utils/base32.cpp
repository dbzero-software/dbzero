// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "base32.hpp"
#include <cstring>
#include <cassert>
#include <utility>
#include <iostream>

namespace db0

{

    static constexpr const char *base_32_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    std::size_t base32_decode(const char *buf, std::uint8_t *out) noexcept
    {            
        // decode table contains mask + offset
        static constexpr std::size_t dtable_size = 12;
        static constexpr std::pair<std::uint8_t, std::int8_t> dtable[] = {
            { 0b00011111, 3 }, { 0b00011100, -2 }, { 0b00000011, 6 }, { 0b00011111, 1 }, { 0b00010000, -4 },
            { 0b00001111, 4 }, { 0b00011110, -1 }, { 0b00000001, 7 }, { 0b00011111, 2 }, { 0b00011000, -3 },
            { 0b00000111, 5 }, { 0b00011111, 0 }
        };

        // decoding strides
        static constexpr std::uint8_t strides[] = { 2, 3, 2, 3, 2 };

        if (!buf || !*buf) {
            return 0;
        }

        auto dt_pair = dtable, dt_end = dtable + dtable_size;
        auto ptr = out;
        auto stride_ptr = strides;
        auto stride = *stride_ptr;
        auto buf_end = buf + strlen(buf);
        *ptr = 0;
        for (;*buf; ++buf) {
            // FIXME: optimization - table lookup might be faster than scan
            auto cptr = strchr(base_32_chars, *buf);
            if (!cptr) {
                break;
            }
            std::uint8_t value = cptr - base_32_chars;
            for (;;) {
                if (dt_pair->second >= 0) {
                    *ptr |= (value & dt_pair->first) << dt_pair->second;
                    ++dt_pair;
                    if (dt_pair == dt_end) {
                        dt_pair = dtable;
                    }
                    if (--stride == 0 && buf != buf_end - 1) {
                        *(++ptr) = 0;
                        ++stride_ptr;
                        if (stride_ptr == strides + sizeof(strides)) {
                            stride_ptr = strides;
                        }
                        stride = *stride_ptr;
                    }
                    break;
                }

                // underflow, continue with the same value
                *ptr |= (value & dt_pair->first) >> -dt_pair->second;
                ++dt_pair;
                assert(dt_pair != dt_end);
                if (--stride == 0) {
                    ++ptr;
                    *ptr = 0;
                    stride = *++stride_ptr;
                }
            }
        }
        
        return ptr - out + 1;
    }

    std::size_t base32_encode(const std::uint8_t *in, std::size_t size, char *out) noexcept
    {
        static constexpr std::size_t etable_size = 12;
        // encoding table
        static constexpr std::pair<std::uint8_t, std::int8_t> etable[] = {
            { 0b11111000, 3 }, { 0b00000111, -2 }, { 0b11000000, 6 }, { 0b00111110, 1 }, { 0b00000001, -4 },
            { 0b11110000, 4 }, { 0b00001111, -1 }, { 0b10000000, 7 }, { 0b01111100, 2 }, { 0b00000011, -3 },
            { 0b11100000, 5 }, { 0b00011111, 0 }
        };

        // encoding strides
        static constexpr std::uint8_t strides[] = { 1, 2, 1, 2, 2, 1, 2, 1 };

        if (!in || !size) {
            *out = 0;
            return 0;
        }

        char *ptr = out;
        auto end = in + size;
        auto stride_ptr = strides, stride_end = strides + sizeof(strides);
        auto et_pair = etable, et_end = etable + etable_size;
        while (in < end) {
            std::uint8_t enc_value = 0;
            assert(in != end);
            auto in_val = *in;
            for (auto stride = *stride_ptr;stride > 0;--stride) {
                if (et_pair->second > 0) {
                    enc_value |= (in_val & et_pair->first) >> et_pair->second;
                } else {
                    enc_value |= (in_val & et_pair->first) << -et_pair->second;
                    ++in;
                    // pad with 0 when input is not aligned
                    in_val = in != end ? *in : 0;
                }
                ++et_pair;
                if (et_pair == et_end) {
                    et_pair = etable;                    
                }
                assert(enc_value < 32);
            }
            *ptr++ = base_32_chars[enc_value];
            ++stride_ptr;
            if (stride_ptr == stride_end) {
                stride_ptr = strides;
            }
        }
        
        // null-terminate
        *(ptr++) = 0;
        return ptr - out - 1;
    }

}
