// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <optional>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0

{
    
    // Wraps a Python dict object and provides getters for configuration variables
    struct LockFlags
    {
        LockFlags() = default;  
        LockFlags(Config py_logs_flags);
        LockFlags(bool no_lock);
        
        bool m_blocking = false;
        int m_timeout = 0;
        bool m_relock_on_removed_lock = false;
        bool m_force_unlock = false;
        bool m_no_lock = false;
    };
    
}