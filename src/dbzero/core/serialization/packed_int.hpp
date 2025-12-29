// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include "Types.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/compiler_attributes.hpp>
#include <dbzero/core/serialization/bounded_buf_t.hpp>
#include <dbzero/core/memory/config.hpp>

namespace db0

{

DB0_PACKED_BEGIN
    /**
     * @tparam IntT - underlying encoded unsigned integer type
     * @tparam is_nullable flag indicating if this type can be null-ed
     */
    template <class IntT, bool is_nullable = false>
    class DB0_PACKED_ATTR o_packed_int: public o_base<o_packed_int<IntT, is_nullable>, 0, false>
    {
    protected:
        using super_t = o_base<o_packed_int<IntT, is_nullable>, 0, false>;
        friend super_t;

        /// if nullable will create null
        o_packed_int()
        {
            if constexpr (is_nullable) {
                encodeNull((std::byte*)this);
            } else {
                encode(IntT(), (std::byte*)this + measure(IntT()));
            }
        }
        
        o_packed_int(IntT value)
        {
            if constexpr (is_nullable) {
                encodeNullable(value, (std::byte*)this + measure(value));
            } else {
                encode(value, (std::byte*)this + measure(value));
            }
        }

    public:        

        inline IntT value() const
        {
            auto at = reinterpret_cast<const std::byte*>(this);
            return read(at);
        }

        inline operator IntT() const {
            return this->value();
        }
        
        static IntT read(const std::byte *&at)
        {
            if constexpr (is_nullable) {
                return decodeNullable(at);
            } else {
                return decode(at);
            }
        }

        // read with bounds validation
        static IntT read(const std::byte *&at, const std::byte *end)
        {
            const_bounded_buf_t safe_buf(Settings::m_decode_error, at, end);
            if constexpr (is_nullable) {
                auto result = decodeNullable(safe_buf);
                at = safe_buf;
                return result;
            } else {
                auto result = decode(safe_buf);
                at = safe_buf;
                return result;
            }
        }
        
        static void write(std::byte *&at, IntT value)
        {
            auto size_of = measure(value);
            if constexpr (is_nullable) {
                encodeNullable(value, at + size_of);
            } else {
                encode(value, at + size_of);
            }
            at += size_of;
        }
        
        // Write with bounds checked
        static void write(std::byte *&at, IntT value, const std::byte *end)
        {
            auto size_of = measure(value);
            if (at + size_of > end) {
                THROWF(db0::InternalException) << "packed_int overflow";
            }
            if constexpr (is_nullable) {
                encodeNullable(value, at + size_of);
            } else {
                encode(value, at + size_of);
            }
            at += size_of;
        }

        inline bool isNull() const
        {
            if constexpr (is_nullable) {
                return isNull((const std::byte*)this);
            } else {
                return false;
            }
        }

        static std::size_t max_len() {
            return (std::size_t)((sizeof(IntT)*8 - 1)/7) + 1;
        }

        template <class buf_t> static size_t safeSizeOf(buf_t buf) 
        {
            std::size_t max_size = max_len();
            std::size_t size = 1;
            // test continuation bit
            while (static_cast<std::uint8_t>(*buf) & 0x80) {
                ++size;
                ++buf;
                if (size > max_size) {
                    THROWF(db0::InternalException) << "bad data";
                }
            }
            return size;
        }

        static std::size_t measure() {
            return 1u;
        }

        static std::size_t measure(IntT value) 
        {
            std::size_t size = 1;
            if constexpr (is_nullable) {
                // must add additional 1 byte for special null value
                if (value == 0x7f) {
                    ++size;
                }
            }
            value >>= 7;
            while (value) {
                ++size;
                value >>= 7;
            }
            return size;
        }

    private:

        void encodeNull(std::byte *at) {
            *at = static_cast<std::byte>(0x7f);
        }

        inline bool isNull(const std::byte *at) const {
            return *at == static_cast<std::byte>(0x7f);
        }
        
        static void encode(IntT value, std::byte *end)
        {
            // encode bits
            --end;
            // last element without the continuation bit
            *end = static_cast<std::byte>(value & 0x7f);
            value >>= 7;
            while (value) {
                --end;
                // assign continuation bits
                *end = static_cast<std::byte>((value & 0x7f) | 0x80);
                value >>= 7;
            }
        }

        static void encodeNullable(IntT value, std::byte *end)
        {
            if (value == 0x7f) {
                --end;
                // special case, must encode using 2 bytes to distinguish from null
                *end = static_cast<std::byte>(0x7f);
                --end;
                *end = static_cast<std::byte>(0x80);
            } else {
                // encode using regular method otherwise
                encode(value, end);
            }
        }
        
        template <class buf_t> static IntT decode(buf_t &buf)
        {
            IntT value = 0;
            while (static_cast<std::uint8_t>(*buf) & 0x80) {
                value |= (static_cast<std::uint8_t>(*buf) & 0x7f);
                value <<= 7;
                ++buf;
            }
            value |= (static_cast<std::uint8_t>(*buf) & 0x7f);
            ++buf;
            return value;
        }
        
        template <class buf_t> static IntT decodeNullable(buf_t &buf)
        {
            // test for null value
            if (static_cast<std::uint8_t>(*buf) == 0x7f) {
                THROWF(db0::InternalException) << "packed_int unable to decode null";
            }
            // decode using regular method
            return decode(buf);
        }
    };
DB0_PACKED_END

    using packed_int32 = o_packed_int<std::uint32_t>;
    using packed_int64 = o_packed_int<std::uint64_t>;
    using nullable_packed_int32 = o_packed_int<std::uint32_t, true>;
    using nullable_packed_int64 = o_packed_int<std::uint64_t, true>;
    
}
