#pragma once

#include <atomic>
#include <cstdint>

namespace db0

{
    
    template <typename T> void safeSetFlags(std::atomic<T> &value, std::uint32_t flags)
    {
        for (;;)
        {
            auto old_val = value.load();
            if (value.compare_exchange_weak(old_val, old_val | flags)) {
                break;
            }
        }
    }

    template <typename T> void safeResetFlags(std::atomic<T> &value, std::uint32_t flags)
    {
        for (;;)
        {
            auto old_val = value.load();
            if (value.compare_exchange_weak(old_val, old_val & ~flags)) {
                break;
            }
        }
    }
        
}