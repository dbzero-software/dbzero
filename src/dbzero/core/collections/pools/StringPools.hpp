// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <dbzero/core/serialization/string.hpp>
#include "LimitedPool.hpp"
#include "RC_LimitedPool.hpp"
#include <dbzero/core/compiler_attributes.hpp>
    
namespace db0::pools

{
    
    class RC_LimitedStringPool: public RC_LimitedPool<o_string, o_string::comp_t, std::uint32_t>
    {
    public:
        using super_t = RC_LimitedPool<o_string, o_string::comp_t, std::uint32_t>;
        using ItemT = typename super_t::ItemT;
        
        RC_LimitedStringPool(const Memspace &pool_memspace, Memspace &);
        RC_LimitedStringPool(const Memspace &pool_memspace, mptr);

        /**
         * Convenience pointer/element ID type
        */
DB0_PACKED_BEGIN
        struct DB0_PACKED_ATTR PtrT
        {
            std::uint32_t m_value = 0;
            PtrT() = default;
            inline PtrT(std::uint32_t value) : m_value(value) {}

            inline operator bool() const {
                return m_value != 0;
            }

            inline bool operator==(const PtrT &other) const {
                return m_value == other.m_value;
            }

            inline bool operator!=(const PtrT &other) const {
                return m_value != other.m_value;
            }
        };
DB0_PACKED_END
        
        /**
         * Adds a new object or increase ref-count of the existing element
         * @param inc_ref - whether to increase ref-count of the existing element, note that for
         * newly created elements ref-count is always set to 1 (in such case inc_ref will be flipped from false to true)
        */
        PtrT add(bool &inc_ref, const char *);
        PtrT add(bool &inc_ref, const std::string &);
        PtrT addRef(const char *);
        PtrT addRef(const std::string &);
        
        void unRef(PtrT);
        
        // Find existing or return nullptr if does not exist
        PtrT get(const char *) const;
        PtrT get(const std::string &) const;

        std::string fetch(PtrT) const;
        
        /**
         * Convert pointer/element ID to actual memspace address
        */
        std::uint64_t toAddress(PtrT) const;
    };
    
}

namespace db0

{
    
    // limited pool string pointer type
    using LP_String = db0::pools::RC_LimitedStringPool::PtrT;

}
