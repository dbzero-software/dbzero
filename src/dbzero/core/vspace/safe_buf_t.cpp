// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "safe_buf_t.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{
 
    std::function<void()> safe_buf_t::m_bad_address = []() {
        THROWF(db0::BadAddressException) << "Invalid address access";
    };
    
}