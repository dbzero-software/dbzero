#pragma once

#include "packed_int.hpp"

namespace db0

{

    // Pair of packed int-s
    template <typename T1, typename T2>
    class [[gnu::packed]] o_packed_pair: public o_base<o_packed_pair<T1, T2>, 0, false>
    {
    protected:
        using super_t = o_base<o_packed_pair<T1, T2>, 0, false>;
        friend super_t;

        o_packed_pair(T1, T2);

    public:
        static std::size_t measure(T1, T2);
        
        template <typename buf_t> std::size_t safeSizeOf(buf_t at) const
        {
            auto _buf = at;
            at += T1::safeSizeOf(at);
            at += T2::safeSizeOf(at);
            return at - _buf;
        }

        std::pair<T1, T2> value() const;
    };

    template <typename T1, typename T2>
    o_packed_pair<T1, T2>::o_packed_pair(T1 first, T2 second)
    {
        this->arrangeMembers()
            (o_packed_int<T1>::type(), first)
            (o_packed_int<T2>::type(), second);
    }

    template <typename T1, typename T2>
    std::size_t o_packed_pair<T1, T2>::measure(T1 first, T2 second) {
        return o_packed_int<T1>::measure(first) + o_packed_int<T2>::measure(second);
    }
    
    template <typename T1, typename T2>
    std::pair<T1, T2> o_packed_pair<T1, T2>::value() const
    {
        std::byte *at = static_cast<std::byte*>(this);
        auto first = o_packed_int<T1>::read(at);
        auto second = o_packed_int<T2>::read(at);
        return { first, second };
    }

}

