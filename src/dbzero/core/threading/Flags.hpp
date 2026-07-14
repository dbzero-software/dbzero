// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <atomic>
#include <cstdint>

namespace db0

{

    template <typename T> void atomicSetFlags(std::atomic<T> &value, std::uint32_t flags)
    {
        auto old_val = value.load();        
        while (!value.compare_exchange_weak(old_val, old_val | flags));
    }
    
    template <typename T> void atomicResetFlags(std::atomic<T> &value, std::uint32_t flags)
    {
        auto old_val = value.load();
        while (!value.compare_exchange_weak(old_val, old_val & ~flags));
    }
    
    // Check flags and only set if not already set
    // @return false if all flags were already set
    template <typename T> bool atomicCheckAndSetFlags(std::atomic<T> &value, std::uint32_t flags)
    {
        auto old_val = value.load();
        for (;;) {
            // already set by another thread
            if ((old_val & flags) == flags) {
                return false;
            }
            if (value.compare_exchange_strong(old_val, old_val | flags)) {
                return true;
            }            
        }
    }

}