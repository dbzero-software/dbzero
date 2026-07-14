// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdlib>
#include <cstdint>
#include <functional>
#include <vector>
#include <dbzero/core/serialization/bounded_buf_t.hpp>

namespace db0

{
    
    /**
     * bounded_buf_t implementation for v-object access 
     * throwing BadAddressException on failed bounds validation
     */
    class safe_buf_t: public bounded_buf_t
    {
    public :
        safe_buf_t()
            : bounded_buf_t(m_bad_address)
        {
        }

        inline safe_buf_t(std::byte *begin, std::byte *end)
            : bounded_buf_t(m_bad_address, begin, end)
        {
        }

        inline safe_buf_t(std::vector<std::byte> &buf)
            : bounded_buf_t(m_bad_address, buf)
        {
        }
        
        void operator=(const safe_buf_t &buf);

    private :
        static std::function<void()> m_bad_address;
    };

}