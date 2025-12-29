// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "config.hpp"
#include <dbzero/core/exception/Exceptions.hpp>

namespace db0

{
    
#ifndef NDEBUG
    bool Settings::__dbg_logs = false;
    bool Settings::__storage_validation = false;
    unsigned long long Settings::__sleep_interval = 0;
    unsigned int Settings::__write_poison = 0;        
    unsigned int Settings::__dram_io_flush_poison = 0;
#endif 
    
    std::function<void()> Settings::m_decode_error = []() {
        THROWF(db0::IOException) << "Data decoding error: corrupt data detected";
    };
    
    void Settings::reset()
    {
#ifndef NDEBUG        
        __dbg_logs = false;
        __storage_validation = false;
        __sleep_interval = 0;
        __write_poison = 0;
        __dram_io_flush_poison = 0;
#endif
    }

}
