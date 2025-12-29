// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <array>
#include <cstring>
#include "Types.hpp"
#include "packed_int.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN

    // Ref-counter combines 2 packed ints in a fixed-size type
    // this allows for a high combined range while preserving compact size
    template <typename IntT = std::uint32_t, std::size_t SIZEOF = 4>
    class DB0_PACKED_ATTR o_ref_counter: public o_fixed<o_ref_counter<IntT, SIZEOF>>
    {
    private:
        using PackedIntT = o_packed_int<IntT, false>;
        std::array<std::byte, SIZEOF> m_payload;

        const PackedIntT &first() const;

        const PackedIntT &second() const;

    public:
        o_ref_counter();
        o_ref_counter(IntT first, IntT second);

        std::pair<IntT, IntT> get() const;

        IntT getFirst() const;
        IntT getSecond() const;

        void setFirst(IntT value);
        void setSecond(IntT value);
    };
    
    template <typename IntT, std::size_t SIZEOF>
    o_ref_counter<IntT, SIZEOF>::o_ref_counter()
        : o_ref_counter(0, 0)
    {
    }

    template <typename IntT, std::size_t SIZEOF>
    o_ref_counter<IntT, SIZEOF>::o_ref_counter(IntT first, IntT second)        
    {
        auto buf = m_payload.data();
        auto end = buf + SIZEOF;
        PackedIntT::write(buf, first, end);
        PackedIntT::write(buf, second, end);
    }
    
    template <typename IntT, std::size_t SIZEOF>
    const typename o_ref_counter<IntT, SIZEOF>::PackedIntT &o_ref_counter<IntT, SIZEOF>::first() const {
        return PackedIntT::__const_ref(m_payload.data());
    }

    template <typename IntT, std::size_t SIZEOF>
    const typename o_ref_counter<IntT, SIZEOF>::PackedIntT &o_ref_counter<IntT, SIZEOF>::second() const {        
        return PackedIntT::__const_ref(m_payload.data() + this->first().sizeOf());
    }
    
    template <typename IntT, std::size_t SIZEOF>
    std::pair<IntT, IntT> o_ref_counter<IntT, SIZEOF>::get() const
    {
        std::pair<IntT, IntT> result;
        auto buf = m_payload.data();
        result.first = PackedIntT::read(buf);
        result.second = PackedIntT::read(buf);
        return result;
    }

    template <typename IntT, std::size_t SIZEOF>
    IntT o_ref_counter<IntT, SIZEOF>::getFirst() const
    {
        return first().value();
    }

    template <typename IntT, std::size_t SIZEOF>
    IntT o_ref_counter<IntT, SIZEOF>::getSecond() const
    {
        return second().value();
    }

    template <typename IntT, std::size_t SIZEOF>
    void o_ref_counter<IntT, SIZEOF>::setFirst(IntT value)
    {
        int size_diff = static_cast<int>(PackedIntT::measure(value)) - static_cast<int>(first().sizeOf());
        if (size_diff != 0) {
            auto &second = this->second();
            // check overflow
            if (size_diff > 0 && (std::byte*)(&second) + size_diff + second.sizeOf() > m_payload.data() + SIZEOF) {
                THROWF(db0::InternalException) << "ref_counter overflow";
            }
            // need to move the second element
            std::memmove(
                (std::byte*)(&second)+ size_diff,
                &second, 
                second.sizeOf()
            );
        }
        PackedIntT::__new(m_payload.data(), value);
    }
    
    template <typename IntT, std::size_t SIZEOF>
    void o_ref_counter<IntT, SIZEOF>::setSecond(IntT value)
    {
        std::byte *at = (std::byte*)&this->second();
        // write with bounds check
        PackedIntT::write(at, value, m_payload.data() + SIZEOF);
    }

DB0_PACKED_END
}
