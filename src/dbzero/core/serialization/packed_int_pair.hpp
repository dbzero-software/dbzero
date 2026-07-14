// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "packed_int.hpp"
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN
    
    // Pair of packed int-s
    template <typename T1, typename T2>
    class DB0_PACKED_ATTR o_packed_int_pair: public o_base<o_packed_int_pair<T1, T2>, 0, false>
    {
    protected:
        using super_t = o_base<o_packed_int_pair<T1, T2>, 0, false>;
        friend super_t;

        o_packed_int_pair(T1, T2);
        o_packed_int_pair(std::pair<T1, T2> value);

    public:
        static std::size_t measure(T1, T2);

        static std::size_t measure(std::pair<T1, T2> value);
        
        static std::pair<T1, T2> read(const std::byte *&at);

        static void write(std::byte *&at, std::pair<T1, T2> value);

        template <typename buf_t> static std::size_t safeSizeOf(buf_t at)
        {
            auto _buf = at;
            at += o_packed_int<T1, false>::safeSizeOf(at);
            at += o_packed_int<T2, false>::safeSizeOf(at);
            return at - _buf;
        }

        std::pair<T1, T2> value() const;
    };
    
    template <typename T1, typename T2>
    o_packed_int_pair<T1, T2>::o_packed_int_pair(T1 first, T2 second)
    {
        this->arrangeMembers()
            (o_packed_int<T1, false>::type(), first)
            (o_packed_int<T2, false>::type(), second);
    }

    template <typename T1, typename T2>
    o_packed_int_pair<T1, T2>::o_packed_int_pair(std::pair<T1, T2> value)
        : o_packed_int_pair(value.first, value.second)
    {
    }

    template <typename T1, typename T2>
    std::size_t o_packed_int_pair<T1, T2>::measure(T1 first, T2 second) {
        return o_packed_int<T1>::measure(first) + o_packed_int<T2>::measure(second);
    }

    template <typename T1, typename T2>
    std::size_t o_packed_int_pair<T1, T2>::measure(std::pair<T1, T2> value) {
        return measure(value.first, value.second);
    }

    template <typename T1, typename T2>
    std::pair<T1, T2> o_packed_int_pair<T1, T2>::value() const
    {
        const std::byte *at = reinterpret_cast<const std::byte*>(this);
        auto first = o_packed_int<T1, false>::read(at);
        auto second = o_packed_int<T2, false>::read(at);
        return { first, second };
    }

    template <typename T1, typename T2>
    std::pair<T1, T2> o_packed_int_pair<T1, T2>::read(const std::byte *&at)
    {
        auto first = o_packed_int<T1, false>::read(at);
        auto second = o_packed_int<T2, false>::read(at);
        return { first, second };
    }

    template <typename T1, typename T2>
    void o_packed_int_pair<T1, T2>::write(std::byte *&at, std::pair<T1, T2> value)
    {
        o_packed_int<T1, false>::write(at, value.first);
        o_packed_int<T2, false>::write(at, value.second);
    }
        
DB0_PACKED_END
}

