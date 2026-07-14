// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <mutex>
#include <atomic>
#include <thread>
#include <iostream>

namespace db0

{
    
    // Safe recursive mutex with thread tracking
    // allowing additional checks (e.g. for proper integration with Python GIL)
    class SafeRMutex
    {
        std::mutex m_mutex;
        std::atomic<std::thread::id> m_owner =  {};
        int m_recursion_count = 0;
    
    public:
        bool isOwnedByThisThread() const {
            return m_owner.load(std::memory_order_relaxed) == std::this_thread::get_id();
        }

        void lock();
        void unlock();
    };
    
    class SafeRLock
    {
    public:
        SafeRLock() = default;
        SafeRLock(const SafeRLock &) = delete;        
        
        SafeRLock(SafeRLock &&other) noexcept
            : m_mutex_ptr(other.m_mutex_ptr)
        {
            other.m_mutex_ptr = nullptr;
        }

        SafeRLock(SafeRMutex &mutex)
            : m_mutex_ptr(&mutex) 
        {            
            m_mutex_ptr->lock();
        }
        
        ~SafeRLock() {
            unlock();
        }
        
        void unlock()
        {
            if (m_mutex_ptr) {
                m_mutex_ptr->unlock();
                m_mutex_ptr = nullptr;
            }
        }

        SafeRLock &operator=(const SafeRLock &) = delete;
        
        SafeRLock &operator=(SafeRLock &&other) noexcept
        {
            if (this != &other) {
                m_mutex_ptr = other.m_mutex_ptr;
                other.m_mutex_ptr = nullptr;
            }
            return *this;
        }

    private:
        SafeRMutex *m_mutex_ptr = nullptr;
    };

}
