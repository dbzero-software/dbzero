// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025-2026 Wojciech Sebastian Kozlowski

#include "safe_buf_t.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{
 
    std::function<void()> safe_buf_t::m_bad_address = []() {
        THROWF(db0::BadAddressException) << "Invalid address access";
    };
    
}