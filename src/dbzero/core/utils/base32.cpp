#include "base32.hpp"
#include <cstring>
#include <cassert>
#include <utility>

namespace db0

{

    static constexpr const char *base_32_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    std::size_t base32_decode(const char *buf, std::uint8_t *out) noexcept
    {            
        if (!buf || !*buf) {
            return 0;
        }
        
        // mask + offset
        static constexpr std::size_t dtable_size = 12;
        static constexpr std::pair<std::uint8_t, std::int8_t> dtable[] = {
            { 0b00011111, 3 }, { 0b00011100, -2 }, { 0b00000011, 6 }, { 0b00011111, 1 }, { 0b00010000, -4 },
            { 0b00001111, 4 }, { 0b00011110, -1 }, { 0b00000001, 7 }, { 0b00011111, 2 }, { 0b00011000, -3 },
            { 0b00000111, 5 }, { 0b00011111, 0 }
        };
        
        static constexpr std::uint8_t strides[] = { 2, 3, 2, 3, 2 };

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
                if (dt_pair->second > 0) {
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

        return ptr - out;
    }

}
