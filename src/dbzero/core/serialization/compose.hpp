// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "Types.hpp"
#include <dbzero/core/compiler_attributes.hpp>

namespace db0

{
DB0_PACKED_BEGIN
    
    // o_compose allows definign a composite type
    // with a fixed-size and a variable-length components e.g. o_compose<int, o_string>
    template <typename FixedT, typename T> class DB0_PACKED_ATTR o_compose: public o_base<o_compose<FixedT, T>, 0, false>
    { 
    protected:
        using super_t = o_base<o_compose<FixedT, T>, 0, false>;
        friend super_t;

        template <typename... Args> o_compose(const FixedT &first, Args&&... args)
            : m_first(first)
        {
            this->arrangeMembers()
                (T::type(), std::forward<Args>(args)...);
        }

    public:
        FixedT m_first;
        
        const T &second() const {
            return this->getDynFirst(T::type());
        }

        template <typename... Args> static size_t measure(const FixedT &, Args&&... args)
        {
            return super_t::measureMembers()
                (T::type(), std::forward<Args>(args)...);
        }
        
        std::size_t sizeOf() const
        {
            return this->sizeOfMembers()
                (T::type());
        }

        template <typename buf_t> static std::size_t safeSizeOf(buf_t buf)
        {
            auto _buf = buf;
            buf += FixedT::sizeOf();
            buf += T::safeSizeOf(buf);
            return buf - _buf;
        }
    };

DB0_PACKED_END
}
