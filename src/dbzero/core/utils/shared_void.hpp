// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <memory>

namespace db0

{

    /// Construct shared_ptr with deleter
    template <typename T, typename... Args> std::shared_ptr<void> make_shared_void(Args&&... args) 
    {
        return std::shared_ptr<void>(new T(std::forward<Args>(args)...), [](T *ptr) {
            delete ptr;
        });
    }
    
    // A flexible shared_ptr wrapper, can be initialized from shared void
    template <typename T> struct SharedPtrWrapper
    {
        std::shared_ptr<void> m_ptr;
        std::shared_ptr<T> m_shared_ptr;
        T *m_raw_ptr = nullptr;

        SharedPtrWrapper() = default;
        SharedPtrWrapper(std::shared_ptr<void> ptr)
            : m_ptr(ptr) 
            , m_raw_ptr(static_cast<T *>(m_ptr.get()))
        {                        
        }

        SharedPtrWrapper(std::shared_ptr<T> ptr)
            : m_shared_ptr(ptr)            
            , m_raw_ptr(ptr.get())
        {
        }

        inline T *operator->() const { 
            return m_raw_ptr; 
        }

        inline T &operator*() const { 
            return *m_raw_ptr; 
        }

        inline operator bool() const { 
            return m_raw_ptr != nullptr; 
        }
    };
    
} 
