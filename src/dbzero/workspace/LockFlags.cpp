// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "LockFlags.hpp"
#include "Config.hpp"

db0::LockFlags::LockFlags(Config py_logs_flags)
{
    m_blocking = py_logs_flags.get<bool>("blocking", false);
    m_timeout = py_logs_flags.get<long>("timeout", 0);
    m_relock_on_removed_lock = py_logs_flags.get<bool>("relock_on_removed_lock", false);
    m_force_unlock = py_logs_flags.get<bool>("force_unlock", false);
}

db0::LockFlags::LockFlags(bool no_lock)
{
    m_no_lock = no_lock;
}
