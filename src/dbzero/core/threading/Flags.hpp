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
    
}