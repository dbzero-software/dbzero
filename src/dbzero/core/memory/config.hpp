// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include <cstdint>
#include <cstddef>
#include <functional>

namespace db0

{

    /**
     * The minimum allowed page size
     */
    static constexpr std::size_t MIN_PAGE_SIZE = 4096;
    
    // Currently we use uint32_t as the state number which allows enumerating 4B sequential transactions
    // assuming 1s per transaction that gives us 136 years of continuous operation until the state number wraps
    using StateNumType = std::uint32_t;
    
    class Settings
    {
    public:
#ifndef NDEBUG        
        static bool __dbg_logs;
        // performs storage full read / write validation (with in-memory mirroring)
        static bool __storage_validation;
        // sleep interval for time-sensitive tests (e.g. copy_prefix) in milliseconds
        static unsigned long long __sleep_interval;
        // the number of allowed writes before std::abort (or 0 = disabled)
        static unsigned int __write_poison;
        // the number of allowed DRAM_IO flush operations before std::abort (or 0 = disabled)
        static unsigned int __dram_io_flush_poison;
#endif
        // Function to throw the data decoding error (i.e. corrupt data detected)
        static std::function<void()> m_decode_error;

        // reset all settings to default values
        static void reset();
    };
    
}
