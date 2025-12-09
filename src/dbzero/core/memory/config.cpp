// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "config.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{
    
#ifndef NDEBUG
    bool Settings::__dbg_logs = false;
    bool Settings::__storage_validation = false;
#endif 
    
    std::function<void()> Settings::m_decode_error = []() {
        THROWF(db0::IOException) << "Data decoding error: corrupt data detected";
    };

}
