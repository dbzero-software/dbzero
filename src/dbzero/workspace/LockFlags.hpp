#pragma once

#include <optional>
#include <dbzero/object_model/LangConfig.hpp>

namespace db0

{
    
    // Wraps a Python dict object and provides getters for configuration variables
    struct LockFlags
    {     
        LockFlags(Config py_logs_flags);
        LockFlags(bool no_lock);
        LockFlags() = default;
        bool m_blocking = true;
        int m_timeout = 0;
        bool m_relock_on_removed_lock = false;
        bool m_force_unlock = false;
        bool m_no_lock = false;
    };
    
}