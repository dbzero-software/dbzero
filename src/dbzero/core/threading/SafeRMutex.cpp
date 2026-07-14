// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "SafeRMutex.hpp"

namespace db0

{
    
    void SafeRMutex::lock()
    {
        std::thread::id this_id = std::this_thread::get_id();        
        // We can use relaxed ordering because we only care if WE wrote it previously.
        if (m_owner.load(std::memory_order_relaxed) == this_id) {
            ++m_recursion_count;
            return;
        }
        
        m_mutex.lock();        
        m_owner.store(this_id, std::memory_order_relaxed);
        m_recursion_count = 1;
    }
    
    void SafeRMutex::unlock()
    {        
        --m_recursion_count;

        if (m_recursion_count == 0) {
            // Clear ownership BEFORE unlocking to avoid race conditions with future lockers
            m_owner.store(std::thread::id(), std::memory_order_relaxed);
            m_mutex.unlock();
        }
    }

}